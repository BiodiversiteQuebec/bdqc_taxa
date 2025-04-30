# Create a cdpnq_odonates table from the cdpnq odonates data file

# %%
import os
import sys
import pandas as pd
import numpy as np
import sqlite3

# %%
# Set the path to the data directory
xls_path = "scratch/Odonates_CDPNQ_v2024-10-11.xlsx"

# Load the data
df = pd.read_excel(xls_path, header=0)

# Rename and add columns
df = df.rename(columns={"SNAME": "name",
    "AUTHOR_SNOM": "author",
    "SCOMNAME": "vernacular_fr"})

# Keep only the desired columns
df = df[["name","author","vernacular_fr",
        "CLASSE","ORDRE","FAMILLE"]]

# Add the valid_name column
df["valid_name"] = df["name"]

# Add the rank column
df["rank"] = "species"

## Specify the rank for subspecies
df.loc[df['valid_name'].str.split().str.len() == 3, 'rank'] = 'subspecies'

# Add the synonym column
df["synonym"] = False

# Add the canonical_full column
df["canonical_full"] = df["name"] + " " + df["author"]

df.head(20)

# %%
# Create synonym rows

# Case for Hylogomphus adelphus
synonym = df.loc[df["name"] == "Hylogomphus adelphus", :].copy()
synonym["name"] = synonym["name"].str.replace("Hylogomphus", "Gomphus")
synonym["synonym"] = True
df = pd.concat([df, synonym], axis=0)

# Case where name contains genus "Phanogomphus"
synonym = df.loc[df["name"].str.contains("Phanogomphus"), :].copy()
synonym["name"] = synonym["name"].str.replace("Phanogomphus", "Gomphus")
synonym["synonym"] = True
df = pd.concat([df, synonym], axis=0)

# Case where name contains genus "Gomphurus ventricosus"
synonym = df.loc[df["name"].str.contains("Gomphurus ventricosus"), :].copy()
synonym["name"] = synonym["name"].str.replace("Gomphurus", "Gomphus")
synonym["synonym"] = True
df = pd.concat([df, synonym], axis=0)

# Case where name contains genus "Ischnura senegalis senegalensis"
filter = df["name"].str.contains("Ischnura senegalis senegalensis")
df.loc[filter, "name"] = "Ischnura senegalensis"
df.loc[filter, "valid_name"] = "Ischnura senegalensis"

synonym = df.loc[df["name"].str.contains("Ischnura senegalensis"), :].copy()
synonym["name"] = synonym["name"].str.replace("Ischnura senegalensis", "Ischnura senegalis")
synonym["synonym"] = True
df = pd.concat([df, synonym], axis=0)

# Show synonyms
print("Synonyms:")
df.loc[df["synonym"] == True, :]

# %% Append the genuses as rows

# Genus names
genus_rows = df.copy()
genus_rows["name"] = genus_rows["name"].str.split(" ").str[0]
genus_rows["valid_name"] = genus_rows["name"]
genus_rows["rank"] = "genus"
genus_rows["author"] = np.nan
genus_rows["canonical_full"] = genus_rows["name"]
genus_rows["vernacular_fr"] = np.nan

# Drop duplicates
genus_rows = genus_rows.drop_duplicates(subset=["name"])

# Append the genus rows
df = pd.concat([df, genus_rows], axis=0)

print("Genus rows:")
df[df["rank"] == "genus"].head()

# %%
# Write to sqlite database

# Reorder columns
df = df[["name", "valid_name", "rank", "synonym", "author", "canonical_full",
    "vernacular_fr"]]

db_file = "bdqc_taxa/custom_sources.sqlite"
conn = sqlite3.connect(db_file)
# Drop the table if it exists
conn.execute("DROP TABLE IF EXISTS cdpnq_odonates")

# Write the table
df.to_sql("cdpnq_odonates", conn, if_exists="replace", index=False)

# Create fts5 virtual table for full text search
conn.execute("DROP TABLE IF EXISTS cdpnq_odonates_fts")
#c.execute("CREATE VIRTUAL TABLE cdpnq_odonates_fts USING fts5(name, canonical_full, vernacular_fr, vernacular_fr2)")
#c.execute("INSERT INTO cdpnq_odonates_fts (name, canonical_full, vernacular_fr, vernacular_fr2) SELECT name, canonical_full, vernacular_fr, vernacular_fr2 FROM cdpnq_odonates")
conn.commit()
conn.close()

# %%
# Append to the sqlite README file
readme = """

TABLE cdpnq_odonates

Description: 
    This file was generated from the CDPNQ odonates data file.
    The file was obtained from Dominic.Chambers@environnement.gouv.qc.ca on October 11, 2024
    The last version of the odonates xlsx file is from 2024-10-11`.
    The file was parsed using the script `scripts/make_cdpnq_odonates.py`.

Columns:
    name: scientific name
    valid_name: valid scientific name
    rank: rank of the taxa
    synonym: boolean indicating if the name is a synonym
    author: author of the scientific name
    canonical_full: canonical full name
    vernacular_fr: vernacular name in French
    vernacular_fr2: vernacular name in French from Natureserve
"""

with open("..\\bdqc_taxa\\custom_sources.txt", "a") as f:
    f.write(readme)
# %%
