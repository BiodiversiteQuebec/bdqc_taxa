#====================================================================================================
# Create a eliso_invertebrates table from the Eliso data file
#
# Victor Cameron
# 2024-04-23
#
# NOTES
# - The data is stored in an xlsx file with multiple sheets
# - Only one vernacular name is kept for each taxon (when same taxa_name is multiplicated with different vernacular names)
# - The taxa names with comments in parentheses are kept as is but may prevent matching. Waiting on an update from Eliso
#====================================================================================================

# %%
import os
import sys
import pandas as pd
import numpy as np
import sqlite3
import re
import tempfile
from urllib.request import urlretrieve

DB_FILE = "./bdqc_taxa/custom_sources.sqlite"
COLUMNS_TO_SELECT = ['Embranchement', 'Classe', 'Ordre', 'Famille', 'Genre', 'Espèce', 'Nom retenu']

# %%
# Download the xls file from the Eliso website and store in a temp folder
"https://www.eliso.ca/s/Repertoire-des-noms-dInvertebres.xlsx"

# Create temporary folder
temp_dir = tempfile.TemporaryDirectory()

# Download the file
url = "https://www.eliso.ca/s/Repertoire-des-noms-dInvertebres.xlsx"
file_path = temp_dir.name + '/Repertoire-des-noms-dInvertebres.xlsx'
urlretrieve(url, file_path)

# %%
# Format the data

# Read the file and store each sheet in a dictionary
dataframes_dict = pd.read_excel(file_path, sheet_name=None)

# Remove 'Accueil' and 'sources' sheets from the dictionary
dataframes_dict.pop('Accueil')
dataframes_dict.pop('Sources')

# Concatenate dataframes row-wise on desired columns
inverts = pd.concat([df.reindex(columns=COLUMNS_TO_SELECT) for df in dataframes_dict.values()], ignore_index=True)

# %%
# Clean the data

# Remove rows where 'Nom retenu' is empty
inverts = inverts[inverts['Nom retenu'].notna()]

# Remove rows where 'Nom retenu' contains '['
inverts = inverts[~inverts['Nom retenu'].str.contains('\[', na=False)]

# Drop synonyms: Keep only one vernacular name
# In 'Nom retenu', remove the text including and following the first ','
inverts['Nom retenu'] = inverts['Nom retenu'].str.replace(r',.*', '')

# Remove '-' characters that are not followed by other characters from all columns and replace them with NaN
inverts = inverts.replace(r'^-$', np.nan, regex=True)

# Remove white spaces from the beginning and end of all columns
inverts = inverts.apply(lambda x: x.str.strip() if x.dtype == "object" else x)


# %%
# Format taxonomy : rank and taxa_name

# Create a new column 'taxa_rank' with the rank of the taxon based on the first column that is not empty in the row starting with 'Espèce, then 'Genre', 'Famille', 'Ordre', 'Classe', and finally 'Embranchement'
inverts['taxa_rank'] = inverts[['Espèce', 'Genre', 'Famille', 'Ordre', 'Classe', 'Embranchement']].apply(lambda x: x.first_valid_index(), axis=1)
# Translate the rank values to English
inverts['taxa_rank'] = inverts['taxa_rank'].map({'Espèce': 'species', 'Genre': 'genus', 'Famille': 'family', 'Ordre': 'order', 'Classe': 'class', 'Embranchement': 'phylum'})


# Correst espèce column
inverts['Espèce'] = inverts['Genre'] + ' ' + inverts['Espèce']

# For all rows that 'taxa_rank' is not equal to 'species', assign the value of the first column that is not empty in the row starting with 'Genre', 'Famille', 'Ordre', 'Classe', and finally 'Embranchement' to the 'taxa_name' column
inverts['taxa_name'] = inverts[['Espèce', 'Genre', 'Famille', 'Ordre', 'Classe', 'Embranchement']].apply(lambda x: x[x.first_valid_index()], axis=1)

# Drop duplicated names
inverts = inverts.drop_duplicates(subset=["taxa_name"])


# %%
# rename columns

# Rename and add columns
inverts = inverts.rename(columns={"Nom retenu": "vernacular_fr"})

# Reorder the columns
inverts = inverts[['taxa_name', 'vernacular_fr', 'taxa_rank', 'Embranchement', 'Classe', 'Ordre', 'Famille', 'Genre', 'Espèce']]

# %%

# Save the data to a csv file
#inverts.to_csv('./data/eliso_invertebrates.csv', index=False)

# %%
# Write to sqlite database

conn = sqlite3.connect(DB_FILE)
# Drop the table if it exists
conn.execute("DROP TABLE IF EXISTS eliso_invertebrates")

# Write the table
inverts.to_sql("eliso_invertebrates", conn, if_exists="replace", index=False)

# Create fts5 virtual table for full text search
conn.execute("DROP TABLE IF EXISTS eliso_invertebrates_fts")
#c.execute("CREATE VIRTUAL TABLE eliso_invertebrates_fts USING fts5(taxa_name, taxa_rank, vernacular_fr)")
#c.execute("INSERT INTO eliso_invertebrates_fts (taxa_name, taxa_rank, vernacular_fr) SELECT taxa_name, taxa_rank, vernacular_fr FROM eliso_invertebrates")
conn.commit()
conn.close()


# %%
# Test match the database

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

matched_name = 'Gorgonocephalus'

c.execute('''
SELECT eliso_invertebrates.* FROM eliso_invertebrates
JOIN eliso_invertebrates_fts ON eliso_invertebrates_fts.taxa_name = eliso_invertebrates.taxa_name
WHERE eliso_invertebrates_fts MATCH ?
ORDER BY taxa_rank
LIMIT 1
''', (f'"{matched_name}"',))

results = c.fetchall()
print(results)
conn.close()


# %%
# Append to the sqlite README file
readme = """
\nTABLE eliso_invertebrates\n

Description: 
    This file was generated on 2024-04-23 from Eliso's Répertoire des noms d’invertébrés du Québec (2022) file.
    The file was downloaded from https://www.eliso.ca/documents on 2024-04-23.
    The file was parsed using the script `scripts/make_eliso.py`.\n

Columns:
    The file contains a pandas dataframe with the following columns:
    taxa_name: Scientific name of the taxon
    vernacular_fr: French vernacular name of the taxon
    taxa_rank: Taxon rank
    Embranchement: Phylum
    Classe: Class
    Ordre: Order
    Famille: Family
    Genre: Genus
    Espèce: Species\n

Notes:
    The entries have no recorded author.
    The entries may contain comments in parentheses that are kept as is but may prevent matching.
"""

with open("./bdqc_taxa/custom_sources.txt", "a") as f:
    f.write(readme)
