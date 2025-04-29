# Create a cdpnq_vertebrates table from the cdpnq odonates data file

# %%
import os
import sys
import pandas as pd
import numpy as np
import sqlite3

# %%
# Set the path to the data directory
xls_path = "scratch/LFVQ_18_04_2024.xlsx"

# Load the data
df = pd.read_excel(xls_path, header=0)

# %% Prétraiter les entrées avec populations

# Edit column Nom_francais to strip `,pop` and strip spaces
df["Nom_francais"] = df["Nom_francais"].str.split(", pop").str[0].str.strip()

# Same with Nom_anglais
df["Nom_anglais"] = df["Nom_anglais"].str.split(" - ").str[0].str.strip()
df["Nom_anglais"] = df["Nom_anglais"].str.split("(").str[0].str.strip()


# Replace nan values with empty strings in column `SOUS_ESPECE_POP`
df["SOUS_ESPECE_POP"] = df["SOUS_ESPECE_POP"].fillna("")

# Replace all values where `SOUS_ESPECE_POP` contains `pop` with  ""
df.loc[df["SOUS_ESPECE_POP"].str.contains("pop"), "SOUS_ESPECE_POP"] = ""

# %%

# Rename and add columns
df = df.rename(columns={
    "Nom_francais": "vernacular_fr",
    "Nom_anglais": "vernacular_en"
    })

# Create the `name` column using `GENRE`, `ESPECE` and `SOUS_ESPECE_POP`
df["name"] = df["GENRE"] + " " + df["ESPECE"] + " " + df["SOUS_ESPECE_POP"]
df["name"] = df["name"].str.strip()

# Add the valid_name column
df["valid_name"] = df["name"]

# Add the rank column
df["rank"] = "species"

# Change the rank to `subspecies` where `SOUS_ESPECE_POP` is not empty
df.loc[df["SOUS_ESPECE_POP"] != "", "rank"] = "subspecies"

# Add the synonym column
df["synonym"] = False

df["author"] = np.nan

df["canonical_full"] = df["name"]

df = df.drop_duplicates(subset=["name"])

df.head()

# Show rows where `SOUS_ESPECE_POP` is not empty
# df.loc[df["SOUS_ESPECE_POP"] != "", :].head()

# %% Create the synonym rows

# Create a copy of the dataframe
synonym_rows = df.copy()

# Rename `Anciens_noms_scientifiques` to `synonym_names`
synonym_rows = synonym_rows.rename(columns={"Anciens_noms_scientifiques": "synonym_names"})

# Remove rows where `synonym_names` is NaN
synonym_rows = synonym_rows.loc[synonym_rows["synonym_names"].notna(), :]

# Synonym names are stored as a comma or semicolon separated list in the `synonym_names` column
synonym_rows = synonym_rows.rename(columns={"Anciens_noms_scientifiques": "synonym_names"})
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

synonym_rows[["name", "valid_name", "rank", "synonym", "author", "canonical_full", "vernacular_fr", "vernacular_en"]].head()


# %% Append the genuses as rows

# Genus names
genus_rows = df.copy()
genus_rows["name"] = genus_rows["name"].str.split(" ").str[0]
genus_rows["valid_name"] = genus_rows["name"]
genus_rows["rank"] = "genus"
genus_rows["author"] = np.nan

# Remove `petit` or `grand` from the vernacular names
genus_rows["vernacular_fr"] = np.nan
genus_rows["vernacular_en"] = np.nan
genus_rows["canonical_full"] = genus_rows["name"]

# Drop duplicates
genus_rows = genus_rows.drop_duplicates(subset=["name"])

print("Genus rows:")
df[df["rank"] == "genus"].head()


# %% Append the rows and organize columns

# Append the synonym rows
df = pd.concat([df, synonym_rows], axis=0)

# Append the genus rows
df = pd.concat([df, genus_rows], axis=0)

# Drop duplicates
df = df.drop_duplicates(subset=["name"])

# Reorder columns
df = df[["name", "valid_name", "rank", "synonym", "author", "canonical_full", "vernacular_fr", "vernacular_en"]]

# Sort by name
df = df.sort_values(by="name")

df.head(30)

# Manually fix/change Caribou (Rangifer tarandus) matching
df.loc[df['name'] == 'Rangifer tarandus', ['valid_name', 'canonical_full']] = 'Rangifer tarandus caribou'
df.loc[df['name'] == 'Rangifer tarandus', 'rank'] = 'species'
df.loc[df['name'] == 'Rangifer tarandus', 'synonym'] = True
df.loc[df['name'] == 'Rangifer tarandus', 'vernacular_fr'] = 'Caribou des bois'

# %% Export to csv

# Export to csv
df.to_csv("scratch/cdpnq_vertebrates_verified.csv", index=False)

# %%
# Write to sqlite database

db_file = "bdqc_taxa/custom_sources.sqlite"
conn = sqlite3.connect(db_file)
# Drop the table if it exists
conn.execute("DROP TABLE IF EXISTS cdpnq_vertebrates")

# Write the table
df.to_sql("cdpnq_vertebrates", conn, if_exists="replace", index=False)

# Create fts5 virtual table for full text search
conn.execute("DROP TABLE IF EXISTS cdpnq_vertebrates_fts")
#conn.execute("CREATE VIRTUAL TABLE cdpnq_vertebrates_fts USING fts5(name, canonical_full, vernacular_fr, vernacular_en)")
#conn.execute("INSERT INTO cdpnq_vertebrates_fts (name, canonical_full, vernacular_fr, vernacular_en) SELECT name, canonical_full, vernacular_fr, vernacular_en FROM cdpnq_vertebrates")
conn.commit()
conn.close()

# # %%
# # Append to the sqlite README file
# readme = """

# TABLE cdpnq_vertebrates

# Description: 
#     This file was generated from the Liste de la faune vertébrée du Québec (LFVQ) Data file LFVQ_18_04_2024.xlsx 
#     The file was obtained from Données Québec on 2024-04-18.
#     The last version of the file is from 2024-04-18`.
#     The file was parsed using the script `scripts/make_cdpnq_vertebrates.py`.

# Columns:
#     name: scientific name
#     valid_name: valid scientific name
#     rank: rank of the taxa
#     synonym: boolean indicating if the name is a synonym
#     author: author of the scientific name
#     canonical_full: canonical full name
#     vernacular_fr: vernacular name in French
#     vernacular_en: vernacular name in English

# Notes:
#     The entries have no recorded author.
# """

# with open("..\\bdqc_taxa\\custom_sources.txt", "a") as f:
#     f.write(readme)
# %%
