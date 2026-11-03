tmp <- tempfile(fileext = ".xlsx")
httr2::request("https://cdn-contenu.quebec.ca/cdn-contenu/faune/documents/precaire/liste-especes-suivies-CDPNQ.xlsx") |>
  httr2::req_perform(tmp)

# Faune
emv_fauna <- readxl::read_excel(tmp, sheet = "Faune") |>
  dplyr::filter(Situation %in% c("Susceptible", "Vulnérable", "Menacée")) |>
  dplyr::mutate(
    scientific_name = stringr::str_match(
      `Nom commun (Nom scientifique)`,
      "\\((.*)\\)\\s*$"
    )[, 2],
    parent_scientific_name = "Animalia",
    short = dplyr::case_when(
      Situation == "Susceptible" ~ "CDPNQ_SUSC",
      Situation == "Vulnérable" ~ "CDPNQ_VUL",
      Situation == "Menacée" ~ "CDPNQ_ENDANGERED",
      TRUE ~ Situation
    ),
    rank = dplyr::case_when(
      `Niveau taxonomique` == "Espèce" ~ "species",
      `Niveau taxonomique` == "Sous-espèce" ~ "subspecies",
      `Niveau taxonomique` == "Population" ~ "population"
    )
  ) |>
  dplyr::select(short, scientific_name, rank, parent_scientific_name)


# Ancienne version (webscrape)
# CDPNQ_SUSC: 115 (113 dans DB)
# CDPNQ_VUL: 28 (28 dans DB)
# CDPNQ_ENDANGERED: 37 (37 dans DB)

# Nouvelle version (.xlsx)
# CDPNQ_SUSC: 90
# CDPNQ_VUL: 28
# CDPNQ_ENDANGERED: 37

# Flore
emv_flora <- readxl::read_excel(tmp, sheet = "Flore")

# Ancienne version (webscrape)
# CDPNQ_SUSC: 423 (423 dans BD)
# CDPNQ_VUL: 18 (18 dans BD)
# CDPNQ_VUL_HARVEST: 9 (9 dans BD)
# CDPNQ_ENDANGERED: 59 (59 dans BD)

# Nouvelle version (.xlsx)
# CDPNQ_SUSC: 423
# CDPNQ_VUL: 18
# CDPNQ_VUL_HARVEST: 9
# CDPNQ_ENDANGERED: 59

emv_susc <- emv_fauna |> dplyr::filter(short == "CDPNQ_SUSC")
emv_susc_old <- faune_data |> dplyr::filter(short == "CDPNQ_SUSC")
missing_susc <- emv_susc_old[!(emv_susc_old$scientific_name %in% emv_susc$scientific_name), "scientific_name"]

# Gomphus ventricosus présent sur le site comme CDPNQ_SUSC, mais pas dans le .xlsx
# Oceanodroma leucorhoa présent sur le site comme CDPNQ_SUSC, mais pas dans le .xlsx
# Lasiurus cinereus présent sur le site comme CDPNQ_SUSC, mais pas dans le .xlsx
# Thamnophis saurita septentrionalis (.xlsx) vs Thamnophis sauritus (site web)
# Falco peregrinus anatum vs Falco peregrinus tundrius vs pop1 sur la même ligne dans .xlsx
# Cistothorus stellaris (.xlsx) vs Cistothorus platensis (site web)

# Esox americanus vermiculatus (ajouter Esox americanus?)
# Salvelinus alpinus oquassa (ajouter Salvelinus alpinus?)
# Diadophis punctatus edwardsii (ajouter Diadophis punctatus?)
# Nerodia sipedon sipedon (ajouter Nerodia sipedon?)
# Thamnophis saurita septentrionalis (ajouter Thamnophis saurita?)
# Calidris canutus rufa (ajouter Calidris canutus?)
# Ammodramus savannarum pratensis (ajouter Ammodramus savannarum?)


# Use-case Francois
# sp split
# becassine de wilson vs becassine des marais
# tiarella cordofolia vs tiarella stolonifera
# larus argentatus vs larus hudsonianus ou mithsonianus

# diff status
# glycerie de fernalde
# torreyocloa palida palida vs  torreyocloa palida fernaldi
# ergieron philadelphicus philadelphicus vs erigeron philadelphicus provancheri



################################################################################
# This script updates the rubus.taxa_groups_members table
#
# 1. This script extracts the list of threatened species in Quebec from the Quebec
# government website. The data is extracted from the following pages:
# - Faune: https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques
# - Flore: https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/flore
# the data is prepared for injection in the rubus.taxa_group_members table of the Atlas database
#
# 2. This script extracts the list of invasive species in Quebec from the Quebec
# government website. The data is extracted from the following pages:
# - Liste des principales espèces exotiques envahissantes: https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques/gestion-especes-exotiques-envahissantes-animales/liste-especes
# - Liste des espèces exotiques envahissantes répertoriées dans Sentinelle: https://www.donneesquebec.ca/recherche/dataset/31f841b6-a544-47f9-93fb-b111a46fc654/resource/ac4aeddf-13ed-4d80-9ca3-28ca9ed77b14/download/sentinelle_liste_sp.csv
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

################################################################################
# 1. CDPNQ EMV
################################################################################

#================================================================================================
# 0. Setup
#================================================================================================
library(rvest)
library(RPostgres)

# Load .env file
readRenviron(".env")

# Constants
url_faune <- "https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques/especes-fauniques-menacees-vulnerables/liste"
div_faune_lists <- c(
  "CDPNQ_ENDANGERED" = "#c159706",
  "CDPNQ_VUL" = "#c159753",
  "CDPNQ_SUSC" = "#c159756"
)

url_flore <- "https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/flore/especes-floristiques-menacees-ou-vulnerables/liste-especes"
div_flore_lists <- c(
  "CDPNQ_ENDANGERED" = "#c293495",
  "CDPNQ_VUL" = "#c293496",
  "CDPNQ_VUL_HARVEST" = "#c293497",
  "CDPNQ_SUSC" = "#c293498"
)

#================================================================================================
# 1. Functions
#================================================================================================

extract_table_data <- function(table, list_name) {
  vernacular_name <- table |> html_nodes("tbody tr td:nth-child(1)") |> html_text(trim = TRUE)
  scientific_name <- table |> html_nodes("tbody tr td:nth-child(2)") |> html_text(trim = TRUE)
  category_faune <- table |> html_node("caption") |> html_text(trim = TRUE)
  category_flore <- table |> html_node("h3") |> html_text(trim = TRUE)
  category <- ifelse(!is.na(category_faune), category_faune, category_flore)
  parent_scientific_name <- ifelse(!is.na(category_faune), "Animalia", "Plantae")

  data.frame(vernacular_fr = vernacular_name,
             scientific_name = scientific_name,
             short = list_name,
             category = category,
             parent_scientific_name = parent_scientific_name,
             stringsAsFactors = FALSE)
}

define_taxonomic_level <- function(data) {
  data <- data |>
    dplyr::mutate(rank = dplyr::case_when(
      #grepl("population", vernacular_fr, ignore.case = TRUE) ~ "population",
      grepl("var\\.", scientific_name, ignore.case = TRUE) ~ "variety",
      grepl("subsp\\.", scientific_name, ignore.case = TRUE) ~ "subspecies",
      nchar(gsub("[^ ]", "", scientific_name, ignore.case = TRUE)) == 2 ~ "subspecies",
      TRUE ~ "species"
    ))
}

extract_data <- function(url, div_lists) {
  page <- read_html(url)
  data <- lapply(names(div_lists), function(list_name) {
    div_id <- div_lists[[list_name]]
    tables <- page |> html_elements(div_id) |> html_elements("table")
    lapply(tables, extract_table_data, list_name = list_name)
  }) |>
    dplyr::bind_rows()
  data <- define_taxonomic_level(data)
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
# Extract data for faune and flore
faune_data <- extract_data(url_faune, div_faune_lists) |> tibble::as_tibble() |> dplyr::select(short, scientific_name, rank, parent_scientific_name)
flore_data <- extract_data(url_flore, div_flore_lists)

# Combine the data
cdpnq_emv <- rbind(faune_data, flore_data)

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


################################################################################
# 2. Invasive species
#
# Deux sources :
# - Liste des principales espèces exotiques envahissantes: https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques/gestion-especes-exotiques-envahissantes-animales/liste-especes
# - Liste des espèces exotiques envahissantes répertoriées dans Sentinelle: https://www.donneesquebec.ca/recherche/dataset/31f841b6-a544-47f9-93fb-b111a46fc654/resource/ac4aeddf-13ed-4d80-9ca3-28ca9ed77b14/download/sentinelle_liste_sp.csv
#
# - La présence d’une EEE est répertoriée selon laiste des principales espèces exotiques envahissantes dans les catégories suivantes :
#     Absente du Québec : l’espèce n’a jamais été observée au Québec;
#     Observations ponctuelles : l’espèce a été observée à certaines occasions, mais il n’y a pas de preuve que les individus survivent et se reproduisent de manière autonome de sorte à former une population;
#     Observations récurrentes : l’espèce a été observée à plusieurs occasions, mais  il n’y a pas de preuve que les individus survivent et se reproduisent de manière autonome de sorte à former une population;
#     Établie : l’espèce est observée et il y a des preuves ou de bonnes raisons de croire que les individus survivent et se reproduisent de manière autonome de sorte à former une population.
################################################################################

#================================================================================================
# 0. Setup
#================================================================================================

library(rvest)
library(RPostgres)

# Load .env file
readRenviron(".env")

# Constants
url_liste_qc <- "https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques/gestion-especes-exotiques-envahissantes-animales/liste-especes"
div_tables <- c(
  "faune" = "#c306328",
  "flore" = "#c306329",
  "champignons" = "#c150579"
)

url_sentinelle <- "https://stqc380donopppdtce01.blob.core.windows.net/donnees-ouvertes/Especes_exo_envahissantes/especes_exo_envahissantes.json"

url_aquatique <- "https://diffusion.mffp.gouv.qc.ca/Diffusion/DonneeGratuite/Faune/EAE_faunique/CSV/BD_EAE_faunique_Quebec.csv"

#================================================================================================
# 1. Functions
#================================================================================================

# Function to extract data from the principal invasive species list
extract_data_principal <- function(table, list_name) {
  scientific_name <- table |> html_nodes("tbody tr td:nth-child(2)") |> html_text(trim = TRUE)
  parent_scientific_name <- ifelse(list_name == "faune", "Animalia", ifelse(list_name == "flore", "Plantae", "Fungi"))

  data.frame(scientific_name = stringr::str_trim(scientific_name),
             short = "PRINCIPAL_INVASIVE",
             parent_scientific_name = parent_scientific_name,
             stringsAsFactors = FALSE)
}

# Function to extract data from Sentinelle
extract_data_sentinelle <- function(url) {
  sentinelle_data <- jsonlite::fromJSON(url)$features$properties |>
    dplyr::select(scientific_name = Nom_espece_latin) |>
    dplyr::distinct() |>
    dplyr::mutate(short = "SENTINELLE_INVASIVE",
                  parent_scientific_name = NA,
                  scientific_name = stringr::str_trim(scientific_name))
}

# Function to extract data from Aquatique envahissante
extract_data_aquatic <- function(url) {
  aquatic_data <- read.csv(url) |>
    dplyr::select(scientific_name = especes) |>
    dplyr::distinct() |>
    dplyr::mutate(short = "AQUATIC_INVASIVE",
                  parent_scientific_name = NA,
                  scientific_name = stringr::str_trim(scientific_name))

}

# Function to clean and fix scientific names
clean_scientific_names <- function(data) {
  data[data$scientific_name == "Lymantria dispar asiatica, L. dispar japonica", "scientific_name"] <- "Lymantria dispar asiatica"
  data <- rbind(data, data.frame(scientific_name = "Lymantria dispar japonica", short = "PRINCIPAL_INVASIVE", parent_scientific_name = "Animalia", stringsAsFactors = FALSE))
  data <- data[!is.na(data$scientific_name), ]
  data <- data[data$scientific_name != "", ]
}

#================================================================================================
# 2. Main Script
#================================================================================================

# Extract data for principal invasive species
page <- read_html(url_liste_qc)
eee_principales_data <- lapply(names(div_tables), function(list_name) {
  div_id <- div_tables[[list_name]]
  tables <- page |> html_nodes(paste0(div_id, " table"))
  lapply(tables, function(tbl) extract_data_principal(tbl, list_name = list_name))
}) |>
  dplyr::bind_rows()
eee_principales_data <- clean_scientific_names(eee_principales_data) |>
  define_taxonomic_level()

# Extract data from Sentinelle
eee_sentinelle_data <- extract_data_sentinelle(url_sentinelle) |>
  define_taxonomic_level()

# Extract data from Aquatique envahissante
eee_aquatic_data <- extract_data_aquatic(url_aquatique) |>
  define_taxonomic_level()

# Combine the data and remove duplicates
eee_all <- dplyr::bind_rows(eee_principales_data, eee_sentinelle_data, eee_aquatic_data) |>
  dplyr::distinct(scientific_name, .keep_all = TRUE) |>
  dplyr::mutate(short = "INVASIVE_SPECIES")

# Connect to the database
print("Connecting to Database…")
con <- dbConnect(Postgres(), dbname = Sys.getenv("POSTGRES_DB"),
                 host = Sys.getenv("POSTGRES_HOST"), port = Sys.getenv("POSTGRES_PORT"),
                 user = Sys.getenv("POSTGRES_USER"), password = Sys.getenv("POSTGRES_PASSWORD"))

# Delete old data
delete_old_invasive_grp <- "DELETE FROM rubus.taxa_group_members WHERE short IN ('PRINCIPAL_INVASIVE', 'SENTINELLE_INVASIVE', 'INVASIVE_SPECIES', 'AQUATIC_INVASIVE');"
resp_delete_old_invasive <- dbExecute(con, delete_old_invasive_grp)

# Inject new data
inject_data(con, eee_principales_data)
inject_data(con, eee_sentinelle_data)
inject_data(con, eee_aquatic_data)
inject_data(con, eee_all)