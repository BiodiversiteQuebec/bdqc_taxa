#====================================================================================================
# Create a eliso_invertebrates table from the Eliso data file
#
# NOTES
# - The original data is now stored in a GoogleSheets accessible through https://www.repertoirenature.org/documents
# - Instead of tweaking around with authentication to access the GoogleSheets, I downloaded a local version directly
#   from the GoogleSheets online.
# - Only one vernacular name is kept for each taxon (when same taxa_name is multiplicated with different vernacular names)
# - A clean up has been done on scientific names and vernacular names to remove parenthesis, square brackets
#   sp suffix, whitespaces, etc.
#====================================================================================================
# 
import pandas as pd
import sqlite3
pd.set_option('display.max_columns', None)

DB_FILE = "./bdqc_taxa/custom_sources.sqlite"
COLUMNS_TO_SELECT = ['Embranchement', 'Classe', 'Ordre', 'Famille', 'Genre', 'Espèce', 'Nom scientifique', 'Nom français']

# Format the data
# Read the file and store each sheet in a dictionary
eliso = pd.read_excel("scratch/Répertoire des noms français des invertébrés du Québec.xlsx", sheet_name=None, na_values=["-"])
eliso.pop('Accueil')

# Format
# Concatenate dataframes row-wise on desired columns
eliso = pd.concat([df.reindex(columns=COLUMNS_TO_SELECT) for df in eliso.values()], ignore_index=True)

# Rename necessary columns
eliso = eliso.rename(columns={"Nom français": "vernacular_fr"})

# Set the rank based on the more precise value
eliso['taxa_rank'] = eliso[['Espèce', 'Genre', 'Famille', 'Ordre', 'Classe', 'Embranchement']].apply(lambda x: x.first_valid_index(), axis=1)

# Translate the rank to english
eliso['taxa_rank'] = eliso['taxa_rank'].map({'Espèce': 'species', 'Genre': 'genus', 'Famille': 'family', 'Ordre': 'order', 'Classe': 'class', 'Embranchement': 'phylum'})

# Set the taxa_name
eliso['taxa_name'] = eliso[['Nom scientifique', 'Genre', 'Famille', 'Ordre', 'Classe', 'Embranchement']].apply(lambda x: x[x.first_valid_index()], axis=1)

# Remove parenthesis, sp. and whitespaces in taxa_name
eliso["taxa_name"] = (
    eliso["taxa_name"]
    .str.replace(r"\s*\([^)]*\)", "", regex=True)
    .str.replace(r"\s+sp\.$", "", regex=True)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

# Remove duplicated juveniles stade entries (keep adult one)
eliso = eliso[
    ~(
        eliso["taxa_name"].duplicated(keep=False)
        & eliso["vernacular_fr"].str.contains(r"\bgalles?\b", case=False, na=False)
    )
]

eliso = eliso[
    ~(
        eliso["taxa_name"].duplicated(keep=False)
        & eliso["vernacular_fr"].str.contains(r"\blarve\b", na=False)
    )
]

eliso = eliso[
    ~(
        eliso["taxa_name"].duplicated(keep=False)
        & eliso["vernacular_fr"].str.contains(r"Arpenteuse", na=False)
    )
]

# Remove [, parenthesis and whitespaces in vernacular_fr
eliso["vernacular_fr"] = (
    eliso["vernacular_fr"]
    .str.replace(r"\s*\[[^\]]*\]", "", regex=True)   
    .str.replace(r"\s*\([^)]*\)", "", regex=True)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()   
)

# Remove specific cases that are just wrong
eliso = eliso[~(eliso["taxa_name"] == "A…")]

# Remove the other duplicates just by keeping the first occurrence
eliso = eliso.drop_duplicates(subset=["taxa_name", "taxa_rank"], keep="first")

# Remove white spaces from the beginning and end of all columns
eliso = eliso.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Reorder the columns
eliso = eliso[['taxa_name', 'vernacular_fr', 'taxa_rank', 'Embranchement', 'Classe', 'Ordre', 'Famille', 'Genre', 'Espèce']]


# Write to sqlite database
conn = sqlite3.connect(DB_FILE)

# Drop the table if it exists
conn.execute("DROP TABLE IF EXISTS eliso_invertebrates")

# Write the table
eliso.to_sql("eliso_invertebrates", conn, if_exists="replace", index=False)

# Create fts5 virtual table for full text search
conn.execute("DROP TABLE IF EXISTS eliso_invertebrates_fts")
#c.execute("CREATE VIRTUAL TABLE eliso_invertebrates_fts USING fts5(taxa_name, taxa_rank, vernacular_fr)")
#c.execute("INSERT INTO eliso_invertebrates_fts (taxa_name, taxa_rank, vernacular_fr) SELECT taxa_name, taxa_rank, vernacular_fr FROM eliso_invertebrates")
conn.commit()
conn.close()


# Append to the sqlite README file
readme = """
\nTABLE eliso_invertebrates\n

Description: 
    This file was generated on 2026-05-12 from Eliso's Répertoire des noms d'invertébrés du Québec (2025) file.
    The file was downloaded from https://www.repertoirenature.org/documents on 2026-05-12.
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
