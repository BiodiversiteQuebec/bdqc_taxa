# Create a cdpnq_vertebrates table from the cdpnq odonates data file

# %%
import pandas as pd
import numpy as np
import sqlite3
from bdqc_taxa.gbif import Species
import concurrent.futures
import os

# Set the path to the file directory
file_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(file_dir)

# %%
# Set the path to the data directory
xls_path = "scratch/LFVQ_31_01_2025.xlsx"
# Load the data
df = pd.read_excel(xls_path, header=0, sheet_name="LFVQ_31_01_2025")

# %% Preformat the dataframe

# Rename and add columns
df = df.rename(columns={
    "Nom_francais": "vernacular_fr",
    "Nom_anglais": "vernacular_en",
    "Anciens_noms_scientifiques": "synonym_names",
    })

# Modify the ESPECE column for better usage
df["ESPECE"] = df["GENRE"].str.strip() + " " + df["ESPECE"].str.strip()

# Create the `name` column using `GENRE`, `ESPECE` and `SOUS_ESPECE_POP`
df["name"] = df["Nom_scientifique"].str.strip()

# Add the valid_name column
df["valid_name"] = df["name"]

# Add the rank column
df["rank"] = "species"

# Change the rank to `subspecies` where `SOUS_ESPECE_POP` is not empty
df.loc[df["SOUS_ESPECE_POP"].notna(), "rank"] = "subspecies"

# Change the rank to `population` where `SOUS_ESPECE_POP` contains `pop`
df.loc[df["SOUS_ESPECE_POP"].str.contains("pop", case=False, na=False), "rank"] = "population"

# Add the synonym column
df["synonym"] = False
df["author"] = np.nan

# Keep only the relevant columns
df = df[[
    "ELEMENT_ID",
    "name",
    "valid_name",
    "rank",
    "synonym",
    "author",
    "vernacular_fr",
    "vernacular_en",
    "CLASSE",
    "ORDRE",
    "FAMILLE",
    "GENRE",
    "ESPECE",
    "SOUS_ESPECE_POP",
    "synonym_names",
    "Origine",
    "Rang_S",
]
]

# Create new column for valid_genus
df["GENRE_VALIDE"] = df["GENRE"].str.strip()

# %% Créer les entrées pour les espèces avec populations qui n'existent pas sans mentions de pop
# Note : Seulement le Rangifer tarandus caribou est un ssp qui possède des entrées avec des mentions de pop, et possède déjà des entrées sans mentions de pop

pop_mask = df["rank"] == "population"

species_has_pop = df.loc[pop_mask, "ESPECE"].groupby(df["ESPECE"]).count() >= 1
df = df.join(
    species_has_pop.rename("species_has_pop"),
    on="ESPECE",
)
df["species_has_pop"] = df["species_has_pop"].fillna(False)

# Create species records when there are only population entries

pop_species = set(df.loc[pop_mask, "ESPECE"].unique())
pop_species_w_records = set(df.loc[df["species_has_pop"] & ~pop_mask, "ESPECE"])

missing_species = pop_species - pop_species_w_records

pop_species_df = df.loc[df["ESPECE"].isin(missing_species), :].copy()

# Remove the `pop` suffix from name
pop_species_df["name"] = pop_species_df["name"].str.split("pop").str[0].str.strip()
pop_species_df["valid_name"] = pop_species_df["name"]

# Edit column Nom_francais to strip `,pop` and strip spaces
pop_species_df["vernacular_fr"] = pop_species_df["vernacular_fr"].str.split(", pop").str[0].str.strip()
pop_species_df["vernacular_fr"] = pop_species_df["vernacular_fr"].str.split(", écotype").str[0].str.strip()

# Same with vernacular_en
pop_species_df["vernacular_en"] = pop_species_df["vernacular_en"].str.split(" - ").str[0].str.strip()
pop_species_df["vernacular_en"] = pop_species_df["vernacular_en"].str.split("(").str[0].str.strip()


# Replace nan values with empty strings in column `SOUS_ESPECE_POP`
pop_species_df["SOUS_ESPECE_POP"] = pop_species_df["SOUS_ESPECE_POP"].fillna("")

# Remove pop suffix from SOUS_ESPECE_POP
pop_species_df["SOUS_ESPECE_POP"] = pop_species_df["SOUS_ESPECE_POP"].str.lower().str.split("pop").str[0].str.strip()

# Sort the dataframe by name, vernacular_fr and vernacular_en with missing values at the end and keep the first occurrence
pop_species_df = pop_species_df.sort_values(by=["name", "vernacular_fr", "vernacular_en"], na_position='last').drop_duplicates(subset=["name"], keep='first')

pop_species_df["rank"] = "species"

# Set column values to NA : ELEMENT_ID, Autres_noms_français, Autres_noms_anglais, Commentaires, Regularite, Degre_de_certitude, Statuts_des_populations, Rang_S, STATUT_LEMV, POPULATION
pop_species_df["ELEMENT_ID"] = np.nan
pop_species_df["Rang_S"] = np.nan
pop_species_df["STATUT_LEMV"] = np.nan
pop_species_df["Origine"] = np.nan
pop_species_df["synonym_names"] = np.nan

# Concatenate the population species dataframe with the main dataframe
df = pd.concat([df, pop_species_df], axis=0).reset_index(drop=True)

# Drop the `species_has_pop` column
df = df.drop(columns=["species_has_pop"], errors='ignore')


# %% Créer les entrées pour les espèces avec sous-espèce qui n'existent pas sans mentions de ssp

ssp_df = df.loc[df["rank"] == "subspecies", :].copy()

ssp_df["name"] = ssp_df["ESPECE"]
ssp_df["name"] = ssp_df["ESPECE"]
ssp_df["valid_name"] = ssp_df["name"]
ssp_df["rank"] = "species"
ssp_df["synonym"] = False

# Count the number of records by ESPECE and set the index name to ESPECE
espece_counts = df["ESPECE"].value_counts().reset_index()

# Join the counts with the subspecies dataframe
ssp_df = ssp_df.merge(espece_counts, left_on="ESPECE", right_on="ESPECE", how="left")

# Change valid name to the ESPECE name when there are multiple subspecies, otherwise keep the subspecies as valid name
#  ie. Rangifer tarandus valid name is Rangifer tarandus caribou
ssp_df.loc[ssp_df["count"] > 1, "valid_name"] = ssp_df["name"]

# Sort the dataframe by name, vernacular_fr and vernacular_en with missing values at the end and keep the first occurrence
ssp_df = ssp_df.sort_values(by=["name", "vernacular_fr", "vernacular_en"], na_position='last').drop_duplicates(subset=["name"], keep='first')


# Set column values to NA : ELEMENT_ID, Autres_noms_français, Autres_noms_anglais, Commentaires, Regularite, Degre_de_certitude, Statuts_des_populations, Rang_S, STATUT_LEMV, POPULATION
ssp_df["ELEMENT_ID"] = np.nan
ssp_df["Rang_S"] = np.nan
ssp_df["STATUT_LEMV"] = np.nan
ssp_df["Origine"] = np.nan
ssp_df["synonym_names"] = np.nan

# Append the subspecies records to the main dataframe
df = pd.concat([df, ssp_df], axis=0).reset_index(drop=True)

# %% Create the gbif_synonyms rows
gbif_synonyms = df.copy()
# Set column values to NA : ELEMENT_ID, Autres_noms_français, Autres_noms_anglais, Commentaires, Regularite, Degre_de_certitude, Statuts_des_populations, Rang_S, STATUT_LEMV, POPULATION
gbif_synonyms["ELEMENT_ID"] = np.nan
gbif_synonyms["Rang_S"] = np.nan
gbif_synonyms["STATUT_LEMV"] = np.nan
gbif_synonyms["Origine"] = np.nan
gbif_synonyms["synonym_names"] = np.nan

def get_gbif_synonyms(name):
    """
    Get the GBIF synonyms for a given name.
    """
    try:
        species = Species.match(name=name, phylum="Chordata")
        # If accepted
        if species and species['status'] == "ACCEPTED":
            return species
        elif species and species['status'] == "SYNONYM":
            species = Species.get(species['acceptedUsageKey'])
            return species
        else:
            return None
    except Exception as e:
        print(f"Error getting GBIF synonyms: {e}")
        return None

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    out_gbif = list(executor.map(get_gbif_synonyms, df["name"].tolist()))

# %% Post-process the GBIF synonyms 

gbif_synonyms["name"] = [s['canonicalName'] if s else None for s in out_gbif]
gbif_synonyms["gbif_rank"] = [s['rank'].lower() if s else None for s in out_gbif]

gbif_synonyms.loc[gbif_synonyms["name"] != gbif_synonyms["valid_name"], "synonym"] = True

gbif_synonyms.loc[gbif_synonyms["rank"]== "population", "synonym"] = False

gbif_synonyms.loc[(gbif_synonyms["rank"] == "subspecies") & (gbif_synonyms["gbif_rank"]== "species"), "synonym"] = False

gbif_synonyms.loc[gbif_synonyms["gbif_rank"].isin(["genus", "family", "order", "class", "phylum"]), "synonym"] = False

gbif_synonyms.loc[gbif_synonyms["gbif_rank"]== "genus", "synonym"] = False


# Remove rows that are not synonyms
gbif_synonyms = gbif_synonyms.loc[gbif_synonyms["synonym"]]

gbif_synonyms["rank"] = gbif_synonyms["gbif_rank"]

def get_species(name):
    parts = name.split(" ")
    if len(parts) >= 2:
        return " ".join(parts[:2])
    else:
        return None
    
def get_subspecies(name):
    parts = name.split(" ")
    if len(parts) == 3:
        return parts[-1]
    elif len(parts) > 3:
        return " ".join(parts[2:])
    
def get_genus(name):
    parts = name.split(" ")
    if len(parts) >= 1:
        return parts[0]
    else:
        return None
gbif_synonyms["GENRE"] = gbif_synonyms["name"].apply(get_genus)
gbif_synonyms["ESPECE"] = gbif_synonyms["name"].apply(get_species)
gbif_synonyms["SOUS_ESPECE_POP"] = gbif_synonyms["name"].apply(get_subspecies)

# Drop the gbif_rank column
gbif_synonyms = gbif_synonyms.drop(columns=["gbif_rank"], errors='ignore')

# Append the gbif_synonyms rows to the main dataframe
df = pd.concat([df, gbif_synonyms], axis=0).reset_index(drop=True)



# %% Create the synonym rows from the Anciens_noms_scientifiques column

# Create a copy of the dataframe
synonym_rows = df.loc[df['synonym_names'].notna()].copy()

# Remove population rows
synonym_rows = synonym_rows.loc[synonym_rows["rank"] != "population", :]

# Synonym names are stored as a comma or semicolon separated list in the `synonym_names` column
synonym_rows["synonym_names"] = synonym_rows["synonym_names"].str.replace(";", ",")
synonym_rows["synonym_names"] = synonym_rows["synonym_names"].str.split(",")

# Remove spaces from the names
synonym_rows["synonym_names"] = synonym_rows["synonym_names"].apply(lambda x: [name.strip() for name in x])

# Remove empty strings from the names
synonym_rows["synonym_names"] = synonym_rows["synonym_names"].apply(lambda x: [name for name in x if name != ""])

# Create a new row for each synonym name
synonym_rows = synonym_rows.explode("synonym_names")

# Remove rows where `synonym_names` is empty
synonym_rows = synonym_rows.loc[synonym_rows["synonym_names"] != "", :]

# Add the `name` column
synonym_rows["name"] = synonym_rows["synonym_names"]
synonym_rows["synonym"] = True

# Set the rank from the number of words in the name
def get_rank(name):
    if len(name.split(" ")) == 1:
        return "genus"
    elif len(name.split(" ")) == 2:
        return "species"
    elif len(name.split(" ")) == 3:
        return "subspecies"
    else:
        return "other"
    
synonym_rows.loc[:, "rank"] = synonym_rows["name"].apply(get_rank)
synonym_rows.loc[:, "GENRE"] = synonym_rows["name"].apply(get_genus)
synonym_rows.loc[:, "ESPECE"] = synonym_rows["name"].apply(get_species)
synonym_rows.loc[:, "SOUS_ESPECE_POP"] = synonym_rows["name"].apply(get_subspecies)

# Set column values to NA : ELEMENT_ID, Autres_noms_français, Autres_noms_anglais, Commentaires, Regularite, Degre_de_certitude, Statuts_des_populations, Rang_S, STATUT_LEMV, POPULATION
synonym_rows["ELEMENT_ID"] = np.nan
synonym_rows["Rang_S"] = np.nan
synonym_rows["STATUT_LEMV"] = np.nan
synonym_rows["Origine"] = np.nan
synonym_rows["synonym_names"] = np.nan

# Concatenate the synonym rows with the main dataframe
df = pd.concat([df, synonym_rows], axis=0).reset_index(drop=True)


# %% Append the genuses as rows

# Genus names
genus_rows = df.copy()
genus_rows["name"] = genus_rows["name"].str.split(" ").str[0]
genus_rows["rank"] = "genus"
genus_rows["author"] = np.nan
genus_rows["ESPECE"] = np.nan
genus_rows["SOUS_ESPECE_POP"] = np.nan
genus_rows["ELEMENT_ID"] = np.nan
genus_rows["Rang_S"] = np.nan
genus_rows["STATUT_LEMV"] = np.nan
genus_rows["synonym_names"] = np.nan
genus_rows["Origine"] = np.nan

# Remove `petit` or `grand` from the vernacular names
genus_rows["vernacular_fr"] = np.nan
genus_rows["vernacular_en"] = np.nan

# Set valid_name for genus rows. It depends if the genus contains all valid species(not synonyms)

genus_rows["valid_name"] = np.nan
genus_valid = set(genus_rows["GENRE_VALIDE"].unique())
genus_rows.loc[genus_rows["name"].isin(genus_valid), "valid_name"] = genus_rows.loc[genus_rows["name"].isin(genus_valid), "name"]

genus_w_uniform_valid = set(genus_rows.groupby("name")["GENRE_VALIDE"].agg('nunique').loc[lambda x: x == 1].index)
genus_invalid_is_uniform = genus_w_uniform_valid - genus_valid
genus_rows.loc[genus_rows["name"].isin(genus_invalid_is_uniform), "valid_name"] = genus_rows.loc[genus_rows["name"].isin(genus_invalid_is_uniform), "GENRE_VALIDE"]

genus_rows = genus_rows.loc[genus_rows["valid_name"].notna(), :]

# Reevaluate the synonym columns from name
genus_rows["synonym"] = genus_rows["name"] != genus_rows["valid_name"]

# All genus should be capitalized
genus_rows["name"] = genus_rows["name"].str.capitalize()

# Drop duplicates
genus_rows = genus_rows.drop_duplicates(subset=["name"])

# Append the genus rows
df = pd.concat([df, genus_rows], axis=0)

# %% Append the rows and organize columns

# Drop duplicates
df = df.drop_duplicates(subset=["name"])

# Rename columns
df = df.rename(columns={
    "GENRE": "genus",
    "ESPECE": "species",
    "Origine": "origin",
    "Rang_S": "s_rank",
})

# Reorder columns
df = df[["name", "valid_name", "rank", "synonym", "author", "vernacular_fr", "vernacular_en", 'genus', 'species', 'origin', 's_rank']]

# Sort by name
df = df.sort_values(by="valid_name")

# Manually fix/change Caribou (Rangifer tarandus) matching
df.loc[df['name'] == 'Rangifer tarandus', 'valid_name'] = 'Rangifer tarandus caribou'
df.loc[df['name'] == 'Rangifer tarandus', 'rank'] = 'species'
df.loc[df['name'] == 'Rangifer tarandus', 'synonym'] = True
df.loc[df['name'] == 'Rangifer tarandus', 'vernacular_fr'] = 'Caribou des bois'

# # Verification
# df.loc[df['valid_name'] == 'Dryobates villosus', :]
# df.loc[df['name'] == 'Leuconotopicus villosus', :]
# df.loc[df['name'] == 'Rangifer tarandus', :]
# df.loc[df['name'] == 'Lasiurus cinereus', :]


# %% Export to csv

# Export to csv
df.to_csv("scratch/cdpnq_vertebrates_verified.csv", index=False)

# %%
# Write to sqlite database
db_file = "../custom_sources.sqlite"
conn = sqlite3.connect(db_file)
# Drop the table if it exists
conn.execute("DROP TABLE IF EXISTS cdpnq_vertebrates")

# Write the table
df.to_sql("cdpnq_vertebrates", conn, if_exists="replace", index=False)

# Create fts5 virtual table for full text search
conn.execute("DROP TABLE IF EXISTS cdpnq_vertebrates_fts")
conn.commit()
conn.close()

# %%
# Append to the sqlite README file
readme = """

TABLE cdpnq_vertebrates

Description: 
    This file was generated from the Liste de la faune vertébrée du Québec (LFVQ) Data file LFVQ_31_01_2025.xlsx.
    The file was obtained from Données Québec on 2025-07-15.
    The last version of the file is from 2025-01-31.
    The file was parsed using the script `scripts/make_cdpnq_vertebrates.py`.

Columns:
    name: scientific name
    valid_name: valid scientific name
    rank: rank of the taxa
    synonym: boolean indicating if the name is a synonym
    author: author of the scientific name
    vernacular_fr: vernacular name in French
    vernacular_en: vernacular name in English
    genus: genus of the taxa
    species: species of the taxa
    origin: origin of the taxa (Indigène, Exotique, Inconnu/Non déterminé)
    s_rank: scientific rank of the taxa (e.g. S1, S2, S3, etc.)

Notes:
    The entries have no recorded author.
"""

with open("..\\custom_sources.txt", "a") as f:
    f.write(readme)
# %%
