# %%
# PARSING THE BRYOQUEL TAXONOMY XLSX FILE

# File structure:
# All data is in the "Liste" sheet
# The first 16 rows are headers
# Headers are in the row 8
# The table is divided by rows of species broken by a merged row containing the clade name
# Columns are:
# 0: id
# 1: family scientific name
# 2: species scientific name
# 3: vernacular name (french)
# 4: vernacular name gender (french)
# 5: vernacular name (english)

# The species scientific name is in the format "Genus species Authorship"

# Returned data should add the clade name as a column
# Returned data should add the authorship as a column

# %%
# Required packages
import pandas as pd
import numpy as np
import re
import tempfile
from urllib.request import urlretrieve

DB_FILE = 'bdqc_taxa/custom_sources.sqlite'

# %%
# Download the xls file from the Bryoquel website and store in a temp folder
# http://societequebecoisedebryologie.org/bryoquel_docs/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx

# Create temporary folder
#temp_dir = tempfile.TemporaryDirectory()

# Download the file
#url = 'http://societequebecoisedebryologie.org/bryoquel_docs/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx'
file_path = 'scratch/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx'
#urlretrieve(url, file_path)

# %%
# Function to parse the authorship from the scientific name

# Example 1: "Barbilophozia lycopodioides (Wallr.) Loeske" -> "(Wallr.) Loeske"
# Example 2: "Anastrophyllum sphenoloboides R.M. Schust." -> "R.M. Schust."

# Returns a tuple with the "Genus species" and the authorship

# Function to parse the authorship from the scientific name

# Example 1: "Barbilophozia lycopodioides (Wallr.) Loeske" -> "(Wallr.) Loeske"
# Example 2: "Anastrophyllum sphenoloboides R.M. Schust." -> "R.M. Schust."
# Example 3: "Sphagnum riparium Ångström" -> "Ångström"

# Returns a tuple with the "Genus species" and the authorship

def parse_authorship(scientific_name):
    # Remove the leading and trailing spaces
    scientific_name = scientific_name.strip()
    
    # Find the second space
    second_space = scientific_name.find(' ', scientific_name.find(' ') + 1)

    # If there is no second space, return the scientific name and an empty string
    if second_space == -1:
        return (scientific_name, '')

    # If there is a second space, return the scientific name and the authorship
    return (scientific_name[:second_space], scientific_name[second_space + 1:])

# Test the function
# print(parse_authorship('Barbilophozia lycopodioides (Wallr.) Loeske'))
# print(parse_authorship('Anastrophyllum sphenoloboides R.M. Schust.'))
# print(parse_authorship('Anastrophyllum sphenoloboides'))
# print(parse_authorship('Hypnum callichroum Brid.'))

# %%
# Read the file
df = pd.read_excel(file_path, sheet_name='Liste', header=7)

# Remove the first 8 rows
df = df.iloc[8:]

# Add the clade name as a column
df['clade'] = np.nan

# Add the authorship as a column
df['authorship'] = np.nan

# Add the species_scientific_name as a column
df['scientific_name'] = np.nan

# Rename the columns
df = df.rename(columns={
    'IDtaxon': 'id',
    'Famille': 'family',
    'Noms latins acceptés': 'canonical_full',
    'Noms français acceptés': 'vernacular_fr',
    'Noms anglais acceptés': 'vernacular_en'
})

# Remove the columns that are not needed
df = df.drop(columns=['Gg', 'QC', 'L'])

# df.head(20)

# %%
# Parse the sheet row by row

# Iterate over the rows
clade_name = ''
for i in range(len(df)):
    # If the row is a merged row, it contains the clade name and IDtaxon is NaN
    if pd.isna(df.iloc[i]['id']):
        clade_name = df.iloc[i]['family']
    
    # If the row is not a merged row, it contains a species
    else:
        # Replace non-breaking spaces with regular spaces
        canonical_name = df.iloc[i]['canonical_full'].replace(u'\xa0', u' ')

        df.iloc[i, df.columns.get_loc('canonical_full')] = canonical_name
        df.iloc[i, df.columns.get_loc('clade')] = clade_name

        try:
            genus_species, authorship = parse_authorship(canonical_name)
            df.iloc[i, df.columns.get_loc('scientific_name')] = genus_species
            df.iloc[i, df.columns.get_loc('authorship')] = authorship
        except IndexError:
            print('Error at row ' + str(i))
            print(df.iloc[i]['scientific_name'])

# Drop the rows that are not species
df = df.dropna(subset=['id'])

# df.head(20)

#  Add rows for genus and family

# Convert the id column to string
df['bryoquel_id'] = ["ID" + "{:03d}".format(int(x)) for x in df['id']]

# Create new rank columns
df['taxon_rank'] = 'species'

# Create new genus_scientific_name columns
df['genus'] = df['scientific_name'].str.split(' ').str[0]

# Add rows for the genus
genus_df = df.copy()
genus_df['scientific_name'] = genus_df['genus']
genus_df['taxon_rank'] = 'genus'

genus_df['bryoquel_id'] = genus_df['scientific_name'].str.lower()
genus_df['canonical_full'] = np.nan
genus_df['authorship'] = np.nan
genus_df['vernacular_fr'] = np.nan
genus_df['vernacular_en'] = np.nan
genus_df = genus_df.drop_duplicates(subset=['scientific_name'])

# Add rows for the family
family_df = genus_df.copy()
family_df['scientific_name'] = family_df['family']
family_df['taxon_rank'] = 'family'

# Set the IDtaxon to the scientific name in lowercase
family_df['bryoquel_id'] = family_df['scientific_name'].str.lower() # TODO this is not string
family_df['genus'] = np.nan
family_df['canonical_full'] = np.nan    
family_df['authorship'] = np.nan
family_df['vernacular_fr'] = np.nan
family_df['vernacular_en'] = np.nan
family_df = family_df.drop_duplicates(subset=['scientific_name'])

# Add genus and family rows to the dataframe
out_df = pd.concat([df, genus_df, family_df])

# Capitalize the scientific names
out_df['scientific_name'] = out_df['scientific_name'].str.capitalize()

# Capitalize the family names
out_df['family'] = out_df['family'].str.capitalize()

# Capitalize the genus names
out_df['genus'] = out_df['genus'].str.capitalize()

# Reorder the columns
out_df = out_df[[
    'bryoquel_id',
    'scientific_name',
    'taxon_rank',
    'genus',
    'family',
    'clade',
    'canonical_full',
    'authorship',
    'vernacular_fr',
    'vernacular_en'
]]

# %%
# Save the dataframe to a csv file
out_df.to_csv('scratch/bryoquel_18_octobre_2024.csv', index=False)

# %%
# Save df to a sqlite database `bdqc_taxa\data\bryoquel_12_septembre_2022.sqlite`
# Required packages
import sqlite3
import os

# Save the dataframe to the database sqlite database with FTS5
conn = sqlite3.connect(DB_FILE)

# Create the table
out_df.to_sql('bryoquel', conn, if_exists='replace')

# %%
# Provide Autocomplete functionnality with FTS5
# https://www.sqlite.org/fts5.html#full_text_index_queries

# Create the FTS5 table
conn.execute("DROP TABLE IF EXISTS bryoquel_fts")
#conn.execute('CREATE VIRTUAL TABLE bryoquel_fts USING fts5(scientific_name, canonical_full, vernacular_fr, vernacular_en)')

# Insert the data
#conn.execute('INSERT INTO bryoquel_fts (scientific_name, canonical_full, vernacular_fr, vernacular_en) SELECT scientific_name, canonical_full, vernacular_fr, vernacular_en FROM bryoquel')
conn.commit()
conn.close()

# %%

# Test match the database

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

matched_name = 'Marsupella sprucei'

c.execute('''
    SELECT * FROM bryoquel
    WHERE scientific_name = ?
    ORDER BY taxon_rank
    LIMIT 1
    ''', (matched_name,))


results = c.fetchall()
print(results)
conn.close()


# %%
# Write a readme file with the same name as the pickle file
readme = """
This is a custom source for bdqc_taxa.
It contains custom taxa list as tables in a sqlite database with FTS5 enabled for autocomplete.

TABLE bryoquel

Description: 
    This file was generated on 2024-10-18 from the Bryoquel taxonomy file.
    The file was downloaded from http://societequebecoisedebryologie.org/Bryoquel.html on 2024-10-18.
    The last version of the bryoquel xlsx file is from 2024-10-18`.
    The file was parsed using the script `scripts/parse_bryoquel.py`.
    The file was parsed using the script parse_bryoquel.ipynb.

Columns:
    The file contains a pandas dataframe with the following columns:
    id: the Bryoquel IDtaxon
    scientific_name: Noms latins acceptés du taxon, sans auteur
    taxon_rank: Taxon rank
    genus: Taxon genus
    family: Taxon family
    clade: Taxon clade
    canonical_full: Noms latins acceptés du taxon, avec auteur
    authorship: Auteur obtenu de Noms latins acceptés
    vernacular_name_fr: Noms français acceptés
    vernacular_name_en: Noms anglais acceptés
"""

# Write the readme file with UTF-8 encoding
with open('../bdqc_taxa/custom_sources.txt', 'w', encoding='utf-8') as f:
    f.write(readme)

# %%
