################################################################################
# Liste des rang S - NatureServe
# Data is currently downloaded manually from https://explorer.natureserve.org/Search
# with filter Location = Quebec and National status in 1, 2, 3 and
# also including subspecies, varieties and populations
################################################################################
library(RPostgres)
readRenviron(".env")

srank_status <- readxl::read_excel("scratch/natureserve_srank_2025-12-09-03-04.xlsx", col_names = TRUE, skip = 1) |>
  dplyr::select(short_group = `Distribution`,
                scientific_name = `Scientific Name`,
                species_group = `Species Group (Broad)`) |>
  dplyr::filter(!is.na(scientific_name) & !(scientific_name %in% c("https://explorer.natureserve.org/AboutTheData", "Rounded State/Provincial Status", "Canada"))) |>
  dplyr::mutate(rank = dplyr::case_when(
    grepl("pop\\.", scientific_name, ignore.case = TRUE) ~ "population",
    grepl("var\\.", scientific_name, ignore.case = TRUE) ~ "variety",
    grepl("ssp\\.", scientific_name, ignore.case = TRUE) ~ "subspecies",
    nchar(gsub("[^ ]", "", scientific_name, ignore.case = TRUE)) == 2 ~ "subspecies",
    TRUE ~ "species"
  ),
  parent_scientific_name = dplyr::case_when(
    species_group %in% c("Vertebrates", "Mussels, Snails, & Other Molluscs", "Insects - Beetles",
                         "Insects - Butterflies and Moths", "Insects - Damselflies and Dragonflies",
                         "Other Invertebrates - Terrestrial/Freshwater") ~ "Animalia",
    species_group %in% c("Vascular Plants - Ferns and relatives", "Vascular Plants - Conifers and relatives",
                         "Vascular Plants - Flowering Plants", "Nonvascular Plants") ~ "Plantae",
    species_group %in% c("Lichens", "Fungi (non-lichenized)") ~ "Fungi",
    TRUE ~ NA_character_
  ),
  short_group = paste0("CDPNQ_", stringr::str_sub(stringr::str_extract(short_group, "(?<=QC \\()[^)]+"), 1, 2))
  ) |>
  dplyr::select(short_group, scientific_name, rank, parent_scientific_name)


# Connect to the database
con <- dbConnect(Postgres(), dbname = Sys.getenv("POSTGRES_DB"),
                 host = Sys.getenv("POSTGRES_HOST"), port = Sys.getenv("POSTGRES_PORT"),
                 user = Sys.getenv("POSTGRES_USER"), password = Sys.getenv("POSTGRES_PASSWORD"))

# Delete old data
dbWithTransaction(con, {
  dbExecute(con, "SET ROLE coleo;")

  delete_resp <- dbExecute(
    con,
    "DELETE FROM rubus.taxa_group_members
     WHERE short IN ('CDPNQ_S1', 'CDPNQ_S2', 'CDPNQ_S3');"
  )

  dbExecute(con, "RESET ROLE;")
})

# Inject the data
inject_query <- "
  SELECT rubus.insert_taxa_obs_group_member(
    short_group:= $1,
    scientific_name := $2,
    rank := $3,
    parent_scientific_name := $4);
  "

dbWithTransaction(con, {
  srank_status |>
    purrr::pwalk(function(short_group, scientific_name, rank, parent_scientific_name) {
      dbExecute(
        con,
        inject_query,
        params = list(
          short_group,
          scientific_name,
          rank,
          parent_scientific_name
        )
      )
    })
})