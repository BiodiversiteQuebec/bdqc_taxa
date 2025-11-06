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
# - ATLAS_STAGING_DATABASE
# - ATLAS_STAGING_HOSTNAME
# - ATLAS_STAGING_PORT
# - ATLAS_STAGING_USER
# - ATLAS_STAGING_PASSWORD
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
library(RPostgreSQL)

# Load .env file
readRenviron("~/.env")

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
    dplyr::mutate(taxonomic_level = dplyr::case_when(
      grepl("population", vernacular_fr, ignore.case = TRUE) ~ "population",
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

inject_data <- function(con, data, group_name = NULL) {
  for (i in seq_len(nrow(data))) {
    ## Insert as its group defined in the short column
    query <- sprintf(
      "INSERT INTO taxa_group_members (short, scientific_name) VALUES ('%s', '%s') ON CONFLICT DO NOTHING;",
      data$short[i], data$scientific_name[i]
    )
    res <- dbExecute(con, query)
    if (res == 0) {
      print(sprintf("The species %s is already in the database", data$scientific_name[i]))
    }
    ## Insert as a member of a larger group if defined
    if (!is.null(group_name)) {
      query <- sprintf(
        "INSERT INTO taxa_group_members (short, scientific_name) VALUES ('%s', '%s') ON CONFLICT DO NOTHING;",
        group_name, data$scientific_name[i]
      )
      res <- dbExecute(con, query)
      if (res == 0) {
        print(sprintf("The species %s is already in the database", data$scientific_name[i]))
      }
    }
    ## Insert into taxa_obs
    query <- sprintf(
      "INSERT INTO taxa_obs (scientific_name, rank, parent_scientific_name) VALUES ('%s', '%s', '%s') ON CONFLICT DO NOTHING;",
      data$scientific_name[i], data$TaxonomicLevel[i], data$parent_scientific_name[i]
    )
    dbExecute(con, query)
  }
}

#================================================================================================
# 2. Main Script
#================================================================================================
# Extract data for faune and flore
faune_data <- extract_data(url_faune, div_faune_lists)
flore_data <- extract_data(url_flore, div_flore_lists)

# Combine the data
cdpnq_emv <- rbind(faune_data, flore_data)

# Display the data
print(CDPNQ_EMV)
write.csv(CDPNQ_EMV, "taxa_group_members_CDPNQ_EMV.csv", row.names = FALSE)

# Connect to the database
drv <- dbDriver("PostgreSQL")
print("Connecting to Database…")
con <- dbConnect(drv, dbname = Sys.getenv("ATLAS_STAGING_DATABASE"),
                host = Sys.getenv("ATLAS_STAGING_HOSTNAME"), port = Sys.getenv("ATLAS_STAGING_PORT"),
                user = Sys.getenv("ATLAS_STAGING_USER"), password = Sys.getenv("ATLAS_STAGING_PASSWORD"))

# Remove old data
DELETE_old_CDPNQ <- "
    DELETE 
    FROM taxa_group_members 
    WHERE short IN ('CDPNQ_ENDANGERED', 'CDPNQ_VUL', 'CDPNQ_SUSC', 'CDPNQ_EMV', 'CDPNQ_VUL_HARVEST');
"
DELETE_old_CDPNQ <- paste(unlist(strsplit(DELETE_old_CDPNQ, "\n")), collapse = " ")
response_delete_old_CDPNQ <- dbExecute(con, DELETE_old_CDPNQ)

# Inject new data
inject_data(con, CDPNQ_EMV, "CDPNQ_EMV")


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
library(dplyr)
library(RPostgreSQL)

# Load .env file
readRenviron("~/.env")

# Constants
URL_LISTE_QC <- "https://www.quebec.ca/agriculture-environnement-et-ressources-naturelles/faune/gestion-faune-habitats-fauniques/gestion-especes-exotiques-envahissantes-animales/liste-especes"
DIV_TABLES <- c(
    "faune"="#c306328",
    "flore"="#c306329",
    "champignons"="#c150579"
)

URL_SENTINELLE <- "https://www.donneesquebec.ca/recherche/dataset/31f841b6-a544-47f9-93fb-b111a46fc654/resource/ac4aeddf-13ed-4d80-9ca3-28ca9ed77b14/download/sentinelle_liste_sp.csv"


#================================================================================================
# 1. Functions
#================================================================================================

# Function to extract data from the principal invasive species list
extract_data_from_principal_list <- function(table, list_name) {
    species <- table %>% html_nodes("tbody tr") %>% html_nodes("td:nth-child(1)") %>% html_text(trim = TRUE)
    scientific_names <- table %>% html_nodes("tbody tr") %>% html_nodes("td:nth-child(2)") %>% html_text(trim = TRUE)
    category_presence <- table %>% html_nodes("tbody tr") %>% html_nodes("td:nth-child(3)") %>% html_text(trim = TRUE)
    parent_scientific_name <- ifelse(list_name == "faune", "Animalia", ifelse(list_name == "flore", "Plantae", "Fungi"))
    data.frame(vernacular_fr = species, scientific_name = scientific_names, short = "PRINCIPAL_INVASIVE", parent_scientific_name=parent_scientific_name, category_presence = category_presence, stringsAsFactors = FALSE)
}

# Function to extract data from Sentinelle
extract_data_from_sentinelle <- function(url) {
    sentinelle_data <- read.csv(url, quote = "", sep=",")
    data <- sentinelle_data %>% select(vernacular_fr = Nom_francais, scientific_name = Nom_latin, parent_scientific_name = Regne) %>% mutate(short = "SENTINELLE_INVASIVE")
    data$scientific_name <- gsub('\"', "", data$scientific_name)
    data <- data[data$vernacular_fr != "Espèce non répertoriée",]
    data$parent_scientific_name <- ifelse(data$parent_scientific_name == "Faune", "Animalia", "Plantae")
    return(data)
}

# Function to clean and fix scientific names
clean_scientific_names <- function(data) {
    data[data$scientific_name == "Lymantria dispar asiatica, L. dispar japonica", "scientific_name"] <- "Lymantria dispar asiatica"
    data <- rbind(data, data.frame(vernacular_fr = "Spongieuse asiatique", scientific_name = "Lymantria dispar japonica", short = "PRINCIPAL_INVASIVE", parent_scientific_name = "Animalia", category_presence = "Absente", stringsAsFactors = FALSE))
    data <- data[!is.na(data$scientific_name),]
    data <- data[data$vernacular_fr != "Vers de terre (regroupe plusieurs espèces)",]
    return(data)
}

# Function to delete old data from the database
delete_old_data <- function(con, group_name) {
    query <- sprintf("DELETE FROM taxa_group_members WHERE short IN ('%s');", group_name)
    dbExecute(con, query)
}

#================================================================================================
# 2. Main Script
#================================================================================================

# Extract data for principal invasive species
page <- read_html(URL_LISTE_QC)
EEE_principales_data <- lapply(names(DIV_TABLES), function(list_name) {
    div_id <- DIV_TABLES[[list_name]]
    tables <- page %>% html_nodes(paste0(div_id, " table"))
    lapply(tables, function(tbl) extract_data_from_principal_list(tbl, list_name = list_name))
}) %>% bind_rows()
EEE_principales_data <- clean_scientific_names(EEE_principales_data)

# Extract data from Sentinelle
EEE_sentinelle_data <- extract_data_from_sentinelle(URL_SENTINELLE)

# Combine the data and remove duplicates
EEE_ALL <- rbind(EEE_principales_data[,c("vernacular_fr", "scientific_name", "short", "parent_scientific_name")], EEE_sentinelle_data[,c("vernacular_fr", "scientific_name", "short", "parent_scientific_name")])
EEE_ALL <- EEE_ALL[!duplicated(EEE_ALL$scientific_name),]
EEE_ALL$short <- "INVASIVE_SPECIES"

# Connect to the database
drv <- dbDriver("PostgreSQL")
print("Connecting to Database…")
con <- dbConnect(drv, dbname = Sys.getenv("ATLAS_STAGING_DATABASE"),
                host = Sys.getenv("ATLAS_STAGING_HOSTNAME"), port = Sys.getenv("ATLAS_STAGING_PORT"),
                user = Sys.getenv("ATLAS_STAGING_USER"), password = Sys.getenv("ATLAS_STAGING_PASSWORD"))

# Delete old data
delete_old_data(con, "PRINCIPAL_INVASIVE")
delete_old_data(con, "SENTINELLE_INVASIVE")
delete_old_data(con, "INVASIVE_SPECIES")

# Inject new data
inject_data(con, EEE_principales_data, "PRINCIPAL_INVASIVE")
inject_data(con, EEE_sentinelle_data, "SENTINELLE_INVASIVE")
inject_data(con, EEE_ALL, "INVASIVE_SPECIES")
