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

# %%
# Download the xls file from the Bryoquel website and store in a temp folder
# http://societequebecoisedebryologie.org/bryoquel_docs/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx

# Create temporary folder
temp_dir = tempfile.TemporaryDirectory()

# Download the file
url = 'http://societequebecoisedebryologie.org/bryoquel_docs/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx'
file_path = temp_dir.name + '/BRYOQUEL_Liste_des_Bryophytes_Qc-Labr.xlsx'
urlretrieve(url, file_path)

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
df['species_scientific_name'] = np.nan

# Rename the columns
df = df.rename(columns={
    'IDtaxon': 'id',
    'Famille': 'family_scientific_name',
    'Noms latins acceptés': 'species_canonical_name',
    'Noms français acceptés': 'vernacular_name_fr',
    'Noms anglais acceptés': 'vernacular_name_en'
})

# Remove the columns that are not needed
df = df.drop(columns=['Gg', 'QC', 'L'])

# df.head(20)

# %%
# Iterate over the rows
clade_name = ''
clade_name = ''
for i in range(len(df)):
    # If the row is a merged row, it contains the clade name and IDtaxon is NaN
    if pd.isna(df.iloc[i]['id']):
        clade_name = df.iloc[i]['family_scientific_name']
    
    # If the row is not a merged row, it contains a species
    else:
        # Replace non-breaking spaces with regular spaces
        canonical_name = df.iloc[i]['species_canonical_name'].replace(u'\xa0', u' ')

        df.loc[i, df.columns.get_loc('species_canonical_name')] = canonical_name
        df.iloc[i, df.columns.get_loc('clade')] = clade_name

        try:
            genus_species, authorship = parse_authorship(canonical_name)
            df.iloc[i, df.columns.get_loc('species_scientific_name')] = genus_species
            df.iloc[i, df.columns.get_loc('authorship')] = authorship
        except IndexError:
            print('Error at row ' + str(i))
            print(df.iloc[i]['species_scientific_name'])

# Drop the rows that are not species
df = df.dropna(subset=['id'])

# Set IDtaxon as an integer
df['id'] = df['id'].astype(int)

# Set the index to IDtaxon
df = df.set_index('id')

# df.head(20)

# %%
# Save df to a sqlite database `bdqc_taxa\data\bryoquel_12_septembre_2022.sqlite` without sqlalchemy

# Required packages
import sqlite3

# Reorder the columns
df = df[[
    'clade',
    'family_scientific_name',
    'species_canonical_name',
    'species_scientific_name',
    'authorship',
    'vernacular_name_fr',
    'vernacular_name_en'
]]


# Connect to the database
conn = sqlite3.connect('bdqc_taxa/bryoquel.sqlite')

# Save the dataframe to the database
df.to_sql('bryoquel', conn, if_exists='replace')

# Close the connection
conn.close()

# Write a readme file with the same name as the pickle file
with open('bdqc_taxa/bryoquel.txt', 'w') as f:
    f.write('This file was generated on 2022-09-21 from the Bryoquel taxonomy file.\n')
    f.write('The file was downloaded from http://societequebecoisedebryologie.org/Bryoquel.html on 2022-09-21.\n')
    f.write('The last version of the bryoquel xlsx file is from 2022-09-12`.\n')
    f.write('The file was parsed using the script `scripts/parse_bryoquel.py`.\n')
    f.write('The file was parsed using the script parse_bryoquel.ipynb.\n')
    f.write('The file contains a pandas dataframe with the following columns:\n')
    f.write('id: the Bryoquel IDtaxon\n')
    f.write('clade: Clade\n')
    f.write('family_scientific_name: Famille\n')
    f.write('species_canonical_name: Noms latins acceptés\n')
    f.write('species_scientific_name: Noms latins acceptés, sans auteur\n')
    f.write('authorship: Auteur obtenu de Noms latins acceptés\n')
    f.write('vernacular_name_fr: Noms français acceptés\n')
    f.write('vernacular_name_en: Noms anglais acceptés\n')

# %%