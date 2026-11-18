################################################################################
# Update the invasive species (EEE) groups in rubus.taxa_group_members
#
# Sources:
# - PRINCIPAL_INVASIVE  : Liste des principales espèces exotiques envahissantes
# - SENTINELLE_INVASIVE : Espèces exotiques envahissantes répertoriées dans Sentinelle
# - AQUATIC_INVASIVE    : Base de données des espèces aquatiques envahissantes fauniques
# - INVASIVE_SPECIES    : Union of the three sources above
#
# Each group is built as a data frame with the columns:
#   scientific_name, rank, parent_scientific_name, short_group
#
# Required environment variables (.env):
#   POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD
#
# Présence au Québec (liste principale):
#   Absente du Québec : l’espèce n’a jamais été observée au Québec;
#   Observations ponctuelles : observée à certaines occasions, sans preuve de population autonome;
#   Observations récurrentes : observée à plusieurs occasions, sans preuve de population autonome;
#   Établie : observée, avec preuves ou bonnes raisons de croire à une population autonome.
################################################################################

#===============================================================================
# 0. Setup
#===============================================================================

library(RPostgres)

readRenviron(".env")

url_principale <- "https://www.donneesquebec.ca/recherche/dataset/42880559-bd24-4de3-99d9-75ff525b43ba/resource/702a5a3c-ef71-42a0-bf6e-01d2d6114f99/download/eee_listeespeces.csv"
url_sentinelle <- "https://stqc380donopppdtce01.blob.core.windows.net/donnees-ouvertes/Especes_exo_envahissantes/especes_exo_envahissantes.json"
url_aquatique  <- "https://diffusion.mffp.gouv.qc.ca/Diffusion/DonneeGratuite/Faune/EAE_faunique/CSV/BD_EAE_faunique_Quebec.csv"

# Règne (liste principale) -> parent_scientific_name
kingdom_map <- c(
  "Faune"       = "Animalia",
  "Flore"       = "Plantae",
  "Champignons" = "Fungi"
)

# Typos in the source files, corrected before insertion
name_fixes <- c(
  "Myopsitta monachus"    = "Myiopsitta monachus",
  "Viviparius georgianus" = "Viviparus georgianus",
  "Lonicera xbella"       = "Lonicera ×bella"
)

# Names dropped from every source after cleaning ("sp."/"spp." already removed).
# Genera that include native species must not be inserted, otherwise all their
# infra-taxa get flagged as invasive.
excluded_names <- c(
  "Lonicera" # Sentinelle "Chèvrefeuille envahissant sp." (LOSP); native Lonicera exist in Québec
)

# Species listed on quebec.ca but missing from the Données Québec CSV
# (checked 2026-11-17 against the page updated 2026-09-29)
principal_additions <- data.frame(
  scientific_name        = "Lymantria dispar dispar",
  parent_scientific_name = "Animalia"
)

group_cols <- c("scientific_name", "rank", "parent_scientific_name", "short_group")

#===============================================================================
# 1. Functions
#===============================================================================

# Normalize raw scientific names:
# - trim and squish whitespace
# - drop parenthetical synonyms, e.g. "Faxonius rusticus (ou Orconectes rusticus)"
# - split comma-separated lists into rows, expanding abbreviated genera
#   ("Lymantria dispar asiatica, L. dispar japonica")
# - normalize hybrid sign ("Reynoutria × bohemica" -> "Reynoutria ×bohemica")
# - apply name_fixes, drop empty names, and remove duplicates
clean_scientific_names <- function(data) {
  data |>
    dplyr::mutate(
      .row = dplyr::row_number(),
      scientific_name = stringr::str_squish(scientific_name),
      scientific_name = stringr::str_remove(scientific_name, "\\s*\\(.*\\)")
    ) |>
    tidyr::separate_longer_delim(scientific_name, delim = ",") |>
    dplyr::mutate(
      scientific_name = stringr::str_squish(scientific_name),
      scientific_name = stringr::str_replace(
        scientific_name, "^[A-Z]\\.(?= )", stringr::word(dplyr::first(scientific_name), 1)
      ),
      .by = .row
    ) |>
    dplyr::select(-.row) |>
    dplyr::mutate(
      scientific_name = stringr::str_replace_all(scientific_name, "× ", "×"),
      scientific_name = dplyr::coalesce(name_fixes[scientific_name], scientific_name)
    ) |>
    dplyr::filter(!is.na(scientific_name), scientific_name != "") |>
    dplyr::distinct()
}

# Assign a rank from the name, and strip "sp."/"spp." from genus-level names
define_taxonomic_level <- function(data) {
  data |>
    dplyr::mutate(
      rank = dplyr::case_when(
        grepl("\\sspp?\\.$", scientific_name) ~ "genus",
        grepl("\\svar\\.\\s", scientific_name) ~ "variety",
        grepl("\\ssubsp\\.\\s", scientific_name) ~ "subspecies",
        stringr::str_count(scientific_name, "\\S+") == 1 ~ "genus",
        stringr::str_count(scientific_name, "\\S+") == 2 ~ "species",
        stringr::str_count(scientific_name, "\\S+") == 3 ~ "subspecies",
        TRUE ~ NA_character_
      ),
      scientific_name = stringr::str_remove(scientific_name, "\\s+spp?\\.$")
    )
}

finalize_group <- function(data, short_group) {
  out <- data |>
    clean_scientific_names() |>
    define_taxonomic_level() |>
    dplyr::filter(!scientific_name %in% excluded_names) |>
    dplyr::mutate(short_group = short_group) |>
    dplyr::select(dplyr::all_of(group_cols)) |>
    dplyr::distinct()

  if (any(is.na(out$rank))) {
    warning(short_group, ": undetermined rank for ",
            paste(out$scientific_name[is.na(out$rank)], collapse = "; "))
  }
  out
}

extract_principal <- function(url) {
  raw <- read.csv(url, fileEncoding = "UTF-8-BOM", check.names = FALSE)

  unknown <- setdiff(unique(raw[["Règne"]]), names(kingdom_map))
  if (length(unknown) > 0) stop("Unknown Règne in liste principale: ", paste(unknown, collapse = ", "))

  data.frame(
    scientific_name        = raw[["Nom latin"]],
    parent_scientific_name = unname(kingdom_map[raw[["Règne"]]])
  ) |>
    dplyr::bind_rows(principal_additions) |>
    finalize_group("PRINCIPAL_INVASIVE")
}

extract_sentinelle <- function(url) {
  jsonlite::fromJSON(url)$features$properties |>
    dplyr::distinct(scientific_name = Nom_espece_latin, Categorie) |>
    dplyr::mutate(parent_scientific_name = ifelse(
      startsWith(stringr::str_trim(Categorie), "Plantes"), "Plantae", "Animalia"
    )) |>
    dplyr::select(scientific_name, parent_scientific_name) |>
    finalize_group("SENTINELLE_INVASIVE")
}

# The aquatic database only contains fauna
extract_aquatic <- function(url) {
  read.csv(url, fileEncoding = "UTF-8-BOM") |>
    dplyr::distinct(scientific_name = especes) |>
    dplyr::mutate(parent_scientific_name = "Animalia") |>
    finalize_group("AQUATIC_INVASIVE")
}

# Union of all sources. A taxa_obs row is identified by
# (scientific_name, rank, parent_scientific_name), so sources must agree on
# rank and parent for the same name to avoid creating duplicate taxa_obs.
build_all_invasive <- function(...) {
  all <- dplyr::bind_rows(...) |>
    dplyr::distinct(scientific_name, rank, parent_scientific_name) |>
    dplyr::mutate(short_group = "INVASIVE_SPECIES")

  conflicts <- all$scientific_name[duplicated(all$scientific_name)]
  if (length(conflicts) > 0) {
    warning("Sources disagree on rank/parent for: ", paste(unique(conflicts), collapse = "; "))
  }
  all
}

inject_data <- function(con, data) {
  query <- "
    SELECT rubus.insert_taxa_obs_group_member(
      short_group := $1,
      scientific_name := $2,
      rank := $3,
      parent_scientific_name := $4);
  "

  rank   <- dplyr::coalesce(data$rank, "")
  parent <- dplyr::coalesce(data$parent_scientific_name, "")

  dbWithTransaction(con, {
    for (i in seq_len(nrow(data))) {
      dbExecute(con, query, params = list(
        data$short_group[i], data$scientific_name[i], rank[i], parent[i]
      ))
    }
  })
  invisible(nrow(data))
}

#===============================================================================
# 2. Main script
#===============================================================================

eee_principal  <- extract_principal(url_principale)
eee_sentinelle <- extract_sentinelle(url_sentinelle)
eee_aquatic    <- extract_aquatic(url_aquatique)
eee_all        <- build_all_invasive(eee_principal, eee_sentinelle, eee_aquatic)

eee_groups <- dplyr::bind_rows(eee_principal, eee_sentinelle, eee_aquatic, eee_all)
print(table(eee_groups$short_group))

print("Connecting to Database…")
con <- dbConnect(Postgres(), dbname = Sys.getenv("POSTGRES_DB"),
                 host = Sys.getenv("POSTGRES_HOST"), port = Sys.getenv("POSTGRES_PORT"),
                 user = Sys.getenv("POSTGRES_USER"), password = Sys.getenv("POSTGRES_PASSWORD"))

# Old members must be deleted beforehand by a role with DELETE on
# rubus.taxa_group_members
#   DELETE FROM rubus.taxa_group_members
#   WHERE short IN ('PRINCIPAL_INVASIVE', 'SENTINELLE_INVASIVE', 'AQUATIC_INVASIVE', 'INVASIVE_SPECIES');
inject_data(con, eee_groups)

dbDisconnect(con)
