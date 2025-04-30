# Match species in the Bryoquel sqlite database

# The bryoquel table has the following columns:
# id: the Bryoquel IDtaxon
# family_scientific_name: Famille
# species_scientific_name: Noms latins accept�s
# vernacular_name_fr: Noms fran�ais accept�s
# vernacular_name_en: Noms anglais accept�s
# clade: Clade
# authorship: Auteur obtenu de Noms latins accept�s


import sqlite3
import importlib.resources
import os.path

# Get the database file from the package data
DB_FILE = 'custom_sources.sqlite'

# Try to find the database file
db_path = None

# Look in parent directory of the package (where it should be after refactoring)
module_dir = os.path.dirname(os.path.dirname(__file__))
db_path = os.path.join(module_dir, DB_FILE)

if not os.path.exists(db_path):
    # Try with importlib.resources as fallback
    try:
        with importlib.resources.open_binary('bdqc_taxa', DB_FILE) as db_file:
            db_path = db_file.name
    except (ImportError, FileNotFoundError):
        raise FileNotFoundError(f"Could not locate {DB_FILE} in package data")

# Connect to the database
conn = sqlite3.connect(db_path)

def match_taxa(species) -> dict:
    """Match a species name to the Bryoquel database
    
    Parameters
    ----------
    species : str
        The species name to match
    
    Returns
    -------
    dict
        A dictionary with the following keys:
        - id: the Bryoquel IDtaxon
        - scientific_name: Noms latins accept�s du taxon, sans auteur
        - taxon_rank: Taxon rank
        - genus: Taxon genus
        - family: Taxon family
        - clade: Taxon clade
        - canonical_full: Noms latins accept�s du taxon, avec auteur
        - authorship: Auteur obtenu de Noms latins accept�s
        - vernacular_name_fr: Noms fran�ais accept�s
        - vernacular_name_en: Noms anglais accept�s
    """
    # Get the cursor
    c = conn.cursor()
    
    # Get the species name
    species = species.strip()

    c.execute('''
    SELECT * FROM bryoquel
    WHERE scientific_name = ?
    ORDER BY taxon_rank
    LIMIT 1
    ''', (species,))

    rows = c.fetchone()

    # If there is a match, return the result
    if rows:
        return {
            'db_id': rows[0],
            'id': rows[1],
            'scientific_name': rows[2],
            'taxon_rank': rows[3],
            'genus': rows[4],
            'family': rows[5],
            'clade': rows[6],
            'canonical_full': rows[7],
            'authorship': rows[8],
            'vernacular_name_fr': rows[9],
            'vernacular_name_en': rows[10]
        }
    # If there is no match, return None
    else:
        return None
