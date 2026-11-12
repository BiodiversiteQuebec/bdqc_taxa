################################################################################
# This script updates the CDPNQ status groups of the rubus.taxa_group_members table
#
# The statuses come from the list of the species followed by the CDPNQ
# (liste-especes-suivies-CDPNQ.xlsx, sheets Faune and Flore). Only the designated statuses
# are kept: Menacée, Vulnérable, Vulnérable à la récolte and Susceptible (not Candidate).
#
# Statuses are injected with the name and the rank given by the CDPNQ (species, subspecies, variety
# or population). A status reaches the observations of the taxon and of its infra-taxa.
#
# Species with infra-taxa (the remaps are done in taxa_ref by bdqc_taxa, cdpnq_vertebrates for
# vertebrates and vascan_remap for vascular plants):
#  - One infra-taxon in Quebec: observations of the species are remapped to it, so they get the
#    status of the infra-taxon without a species-level entry.
#      ie. Nerodia sipedon -> Nerodia sipedon sipedon (Vulnérable)
#    For vertebrates, a species listed itself in the LFVQ is remapped only if the infra-taxon has a status.
#      ie. Osmerus mordax -> Osmerus mordax pop. 1 (Vulnérable)
#    The CDPNQ is trusted over VASCAN for the presence of an infra-taxon in Quebec.
#      ie. Symphyotrichum lanceolatum subsp. lanceolatum is not remapped to var. lanceolatum
#      because var. interior is listed by the CDPNQ, even if VASCAN excludes it from Quebec
#  - Many infra-taxa in Quebec: no remap, each infra-taxon keeps its own status and the species
#    has none, as its observations can't be resolved to an infra-taxon.
#      ie. Esox americanus: only Esox americanus vermiculatus is Vulnérable
#    Except when all the infra-taxa have a status, see inherit_status_taxa.
#  - Taxa that are not vertebrates nor vascular plants (bryophytes, invertebrates) are not remapped.
#
# The status is given to the name used by the CDPNQ, even when VASCAN treats it as a synonym
#  ie. Deschampsia alpina -> Deschampsia cespitosa subsp. neoalpina
#
# Required environment variables:
# - POSTGRES_DB
# - POSTGRES_HOST
# - POSTGRES_PORT
# - POSTGRES_USER
# - POSTGRES_PASSWORD
#
# Author: Victor Cameron
# Date: 2025-01-27
################################################################################

#================================================================================================
# 0. Setup
#================================================================================================
library(RPostgres)

# Load .env file
readRenviron(".env")

# Constants
url_cdpnq_list <- "https://cdn-contenu.quebec.ca/cdn-contenu/faune/documents/precaire/liste-especes-suivies-CDPNQ.xlsx"

cdpnq_groups <- c(
  "Susceptible" = "CDPNQ_SUSC",
  "Vulnérable" = "CDPNQ_VUL",
  "Vulnérable à la récolte" = "CDPNQ_VUL_HARVEST",
  "Menacée" = "CDPNQ_ENDANGERED"
)

# Used to keep one status per taxon, the most severe
status_severity <- c(
  "CDPNQ_SUSC" = 1,
  "CDPNQ_VUL_HARVEST" = 2,
  "CDPNQ_VUL" = 3,
  "CDPNQ_ENDANGERED" = 4
)

# The level is not written the same way in both sheets (ie. Espèce, espèce)
cdpnq_ranks <- c(
  "espèce" = "species",
  "sous-espèce" = "subspecies",
  "variété" = "variety",
  "population" = "population"
)

# Species with no status of their own, many infra-taxa in Quebec, and a status on ALL of them:
# an observation of the species is necessarily of an infra-taxon with a status, so the species gets
# the least severe status of its infra-taxa.
# n_infra is the number of infra-taxa in Quebec, from the LFVQ for vertebrates, VASCAN for vascular
# plants and the CDPNQ names for bryophytes (Bryoquel has no infra-taxa). The script stops if the
# number of infra-taxa with a status changes in the CDPNQ list, the taxon should then be reviewed.
# Species with an infra-taxon without a status are left out, the species has no status
#  ie. Cicuta maculata: var. victorinii is Menacée, but var. maculata has no status
#  ie. Rangifer tarandus caribou: pop. 2 and 14 have a status, pop. 7, 17 and 28 don't
inherit_status_taxa <- tibble::tribble(
  ~scientific_name,        ~rank,     ~parent_scientific_name, ~n_infra,
  "Gadus morhua",          "species", "Animalia",              2, # pop. 4, pop. 7
  "Corallorhiza striata",  "species", "Plantae",               2, # var. striata, var. vreelandii
  "Gentianopsis virgata",  "species", "Plantae",               2, # subsp. macounii, subsp. victorinii
  "Viola sagittata",       "species", "Plantae",               2, # var. ovata, var. sagittata
  "Stegonia latifolia",    "species", "Plantae",               2  # var. latifolia, var. pilifera
)

#================================================================================================
# 1. Functions
#================================================================================================

# Read a sheet of the CDPNQ list into the columns used by inject_data()
read_cdpnq_sheet <- function(path, sheet, parent_scientific_name) {
  readxl::read_excel(path, sheet = sheet) |>
    dplyr::filter(Situation %in% names(cdpnq_groups)) |>
    dplyr::mutate(
      # The scientific name is the last parenthesis of `Nom commun (Nom scientifique)`
      scientific_name = stringr::str_match(`Nom commun (Nom scientifique)`, "\\((.*)\\)\\s*$")[, 2],
      # Flora populations are codes written after the name (ie. Carex macloviana -p11), which
      # seem to be administrative regions (-p11 is the Gaspésie population of Gentianopsis
      # virgata subsp. macounii). The status is given to the taxon, as names with the codes
      # can't be matched and observations can't be resolved to a population
      scientific_name = stringr::str_trim(stringr::str_remove(scientific_name, "\\s+-p\\d.*$")),
      rank = cdpnq_ranks[tolower(`Niveau taxonomique`)],
      rank = dplyr::case_when(
        rank == "population" & grepl("\\svar\\.\\s", scientific_name) ~ "variety",
        rank == "population" & grepl("\\ssubsp\\.\\s", scientific_name) ~ "subspecies",
        rank == "population" & !grepl("\\spop\\.", scientific_name) ~ "species",
        TRUE ~ rank
      ),
      short = unname(cdpnq_groups[Situation]),
      parent_scientific_name = parent_scientific_name
    ) |>
    dplyr::select(short, scientific_name, rank, parent_scientific_name)
}

# Keep one status per taxon, the most severe. A taxon can have many statuses when a flora
# population has a status different from the taxon
#  ie. Erigeron compositus is Susceptible, its populations -p01, p11 are Vulnérable
keep_most_severe <- function(data) {
  data |>
    dplyr::slice_max(status_severity[short], n = 1, with_ties = FALSE, by = scientific_name)
}

# Give the taxa in inherit_status_taxa the least severe status of their infra-taxa
get_inherited_status <- function(data, inherit_taxa) {
  infra <- inherit_taxa |>
    dplyr::cross_join(dplyr::select(data, infra_name = scientific_name, short)) |>
    dplyr::filter(startsWith(infra_name, paste0(scientific_name, " ")))

  # Stop if the infra-taxa with a status are not all the infra-taxa anymore (ie. a status was
  # removed or a name changed in the list)
  n_found <- dplyr::count(infra, scientific_name, name = "n_found")
  wrong_n <- inherit_taxa |>
    dplyr::left_join(n_found, by = "scientific_name") |>
    dplyr::filter(is.na(n_found) | n_found != n_infra)
  if (nrow(wrong_n) > 0) {
    stop("Not all the infra-taxa have a status for: ", paste(wrong_n$scientific_name, collapse = ", "))
  }

  inherited <- infra |>
    dplyr::slice_min(status_severity[short], n = 1, with_ties = FALSE, by = scientific_name) |>
    dplyr::select(short, scientific_name, rank, parent_scientific_name)

  # Stop if a taxon already has a status of its own, it should be removed from inherit_taxa
  has_status <- intersect(inherit_taxa$scientific_name, data$scientific_name)
  if (length(has_status) > 0) {
    stop("Taxa with a status of their own in inherit_status_taxa: ", paste(has_status, collapse = ", "))
  }

  inherited
}

inject_data <- function(con, data) {
  query <- "
  SELECT rubus.insert_taxa_obs_group_member(
    short_group:= $1,
    scientific_name := $2,
    rank := $3,
    parent_scientific_name := $4);
  "

  apply(data, 1, function(x) {
    dbExecute(con, query, params = list(
        x[["short"]],
        x[["scientific_name"]],
        ifelse(is.na(x[["rank"]]), "", x[["rank"]]),
        ifelse(is.na(x[["parent_scientific_name"]]), "", x[["parent_scientific_name"]])
    ))
  })
}

#================================================================================================
# 2. Main Script
#================================================================================================

# Download the CDPNQ list
cdpnq_list_file <- tempfile(fileext = ".xlsx")
httr2::request(url_cdpnq_list) |>
  httr2::req_perform(path = cdpnq_list_file)

emv_fauna <- read_cdpnq_sheet(cdpnq_list_file, "Faune", "Animalia")
emv_flora <- read_cdpnq_sheet(cdpnq_list_file, "Flore", "Plantae")

cdpnq_emv <- dplyr::bind_rows(emv_fauna, emv_flora) |>
  keep_most_severe()

if (any(is.na(cdpnq_emv$scientific_name)) || any(is.na(cdpnq_emv$rank))) {
  stop("Some rows have no scientific name or an unknown taxonomic level")
}

cdpnq_emv <- dplyr::bind_rows(
  cdpnq_emv,
  get_inherited_status(cdpnq_emv, inherit_status_taxa)
)

print(table(cdpnq_emv$parent_scientific_name, cdpnq_emv$short))

# Connect to the database
print("Connecting to Database…")
con <- dbConnect(Postgres(), dbname = Sys.getenv("POSTGRES_DB"),
                 host = Sys.getenv("POSTGRES_HOST"), port = Sys.getenv("POSTGRES_PORT"),
                 user = Sys.getenv("POSTGRES_USER"), password = Sys.getenv("POSTGRES_PASSWORD"))

# Remove old data
delete_old_emv_grp <- "DELETE FROM rubus.taxa_group_members WHERE short IN ('CDPNQ_ENDANGERED', 'CDPNQ_VUL', 'CDPNQ_SUSC', 'CDPNQ_EMV', 'CDPNQ_VUL_HARVEST');"
resp_delete_old_cdpnq <- dbExecute(con, delete_old_emv_grp)

# Inject new data
inject_data(con, cdpnq_emv)

dbDisconnect(con)

#================================================================================================
# Notes
#================================================================================================

# Use-case Francois
# sp split
# becassine de wilson vs becassine des marais
# tiarella cordofolia vs tiarella stolonifera
# larus argentatus vs larus hudsonianus ou mithsonianus

# diff status
# glycerie de fernalde
# torreyocloa palida palida vs  torreyocloa palida fernaldi
# ergieron philadelphicus philadelphicus vs erigeron philadelphicus provancheri
