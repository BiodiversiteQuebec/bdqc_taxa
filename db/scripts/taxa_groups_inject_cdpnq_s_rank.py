# %% Setup connection to database and load environment variables

import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2


INPUT_FILE = "../scratch/liste-especes-suivies-CDPNQ.xlsx"
FLORA_SHEET = "Flore"
FAUNA_SHEET = "Faune"

# Use openpyxl engine to inspect the Excel file
import openpyxl
wb = openpyxl.load_workbook(INPUT_FILE, data_only=True)

print("Parsing Excel file:", INPUT_FILE)
print("Sheet names:", wb.sheetnames)

# PWD
print("Current working directory:", os.getcwd())

ENV_FILE = "../.env.staging"
load_dotenv(ENV_FILE)

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'dbname': os.getenv('POSTGRES_DB')
}

# Print the database configuration to verify that the environment variables are loaded correctly
print("DB_CONFIG:", DB_CONFIG)

def connect():
    # Raise exception if any of the values are empty
    if not all(DB_CONFIG.values()):
        raise ValueError("One or more environment variables are missing")
    
    # Create a connection with proper parameters
    conn_string = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} " \
                  f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} " \
                  f"dbname={DB_CONFIG['dbname']} application_name=atlas-db-unit-tests"
    return psycopg2.connect(conn_string)

# %% Parse the Excel file and load the data into a DataFrame

# Check the first few rows of the Flora sheet
flora_df = pd.read_excel(INPUT_FILE, sheet_name=FLORA_SHEET, engine='openpyxl')
# Add a new column for the sheet name : Royaume
flora_df['Royaume'] = 'Plantae'

print("Flora DataFrame shape:", flora_df.shape)
print("Flora DataFrame dtypes:\n", flora_df.dtypes)
print("Flora DataFrame head:\n", flora_df.head())

fauna_df = pd.read_excel(INPUT_FILE, sheet_name=FAUNA_SHEET, engine='openpyxl')
# Add a new column for the sheet name : Royaume
fauna_df['Royaume'] = 'Animalia'

print("Fauna DataFrame shape:", fauna_df.shape)
print("Fauna DataFrame dtypes:\n", fauna_df.dtypes)
print("Fauna DataFrame head:\n", fauna_df.head())

# Concatenate the Flora and Fauna DataFrames
df = pd.concat([flora_df, fauna_df], ignore_index=True)

# Grand groupe                                         object
# Nom commun (Nom scientifique)                        object
# Situation                                            object
# Rang S                                               object
# Niveau taxonomique                                   object
# Nombre d'occurrences                                  int64
# Occurrence(s) disponible(s)                          object
# Espèce suivie                                        object


# Print unique values of columns of interests : Rang S, Nom commun (Nom scientifique), Situation, Niveau taxonomique
unique_values = {
    'Rang S': df['Rang S'].unique(),
    'Nom commun (Nom scientifique)': df['Nom commun (Nom scientifique)'].unique(),
    'Situation': df['Situation'].unique(),
    'Niveau taxonomique': df['Niveau taxonomique'].unique()
}
print("Unique values in 'Rang S':", unique_values['Rang S'])


# %% Mappings to the database
# Define the mappings for the Flora and Fauna sheets to the database columns

# P

SHORT_GROUP_LOOKUP = {
    'Flore': 'Flore',
    'Faune': 'Faune',
    'Flore et Faune': 'Flore et Faune',
    'Inconnu': 'Inconnu'
}

RANK_LOOKUP = {
    'espèce': 'species',
    'sous-espèce': 'subspecies',
    'variété': 'variety',
    'Population': 'population',
    'genre': 'genus'
}

# Functions to transform the data on a per-row basis
# These functions will be applied to each row of the DataFrame
TRANSFORM_MAPPING_FUNCTIONS = {
    # Functions to transform the data
    'short_group': lambda row: SHORT_GROUP_LOOKUP.get(row['Rang S'], None),
    # Parse parehtheses and extract the scientific name
    'scientific_name': lambda row: row['Nom commun (Nom scientifique)'].split('(')[-1].split(')')[0].strip(),
    'rank': lambda row: RANK_LOOKUP.get(row['Rang S'].lower(), None),
    # No correspondance for author, set to None
    'author': lambda row: None,
    'parent_scientific_name': lambda row: row['kingdom']
}

# Notes de transformation
NOTES = '''
- Rang S : Uniquement prefixes S1, S2, S3, S4, S5 utilisées des valeurs initiales. Valeurs brutes incluent des préfixes (ex. S1?, S1B, S1S2, S4S5B ...)
- Rang S : Rangées ignorées pour les valeurs SH, SNA, SNR, SU, SX
'''
