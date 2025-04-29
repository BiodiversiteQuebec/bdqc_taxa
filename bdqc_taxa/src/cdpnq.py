# Match taxa in the custom_source sqlite database

# The bryoquel table has the following columns:
# name: scientific name
# valid_name: valid scientific name
# rank: rank of the taxa
# synonym: boolean indicating if the name is a synonym
# author: author of the scientific name
# canonical_full: canonical full name
# vernacular_fr: vernacular name in French
# vernacular_fr2: vernacular name in French from Natureserve



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

def match_taxa_odonates(name) -> dict:
    """Match a species name to the Bryoquel database
    Parameters
    ----------
    name : str

    Returns
    -------
    dict
        A dictionary with the following keys:
        name: scientific name
        valid_name: valid scientific name
        rank: rank of the taxa
        synonym: boolean indicating if the name is a synonym
        author: author of the scientific name
        canonical_full: canonical full name
        vernacular_fr: vernacular name in French
        vernacular_fr2: vernacular name in French from Natureserve

    """

    # Get the cursor
    c = conn.cursor()

    # Get the species name
    name = name.strip()
    name = name.replace(',', "")

    c.execute('''
    SELECT * FROM cdpnq_odonates
    WHERE name = ?
    ORDER BY rank
    ''', (name,))

    # Get the first result
    result = c.fetchone()

    # Close the cursor
    c.close()

    # Return the result
    if result:
        return {
            'name': result[0],
            'valid_name': result[1],
            'rank': result[2],
            'synonym': result[3],
            'author': result[4],
            'canonical_full': result[5],
            'vernacular_fr': result[6]
        }
    else:
        return None

def match_taxa_vertebrates(name) -> dict:
    """Match a species name to the CDPQN database
    Parameters
    ----------
    name : str

    Returns
    -------
    dict
        A dictionary with the following keys:
        name: scientific name
        valid_name: valid scientific name
        rank: rank of the taxa
        synonym: boolean indicating if the name is a synonym
        author: author of the scientific name
        canonical_full: canonical full name
        vernacular_fr: vernacular name in French
        vernacular_en: vernacular name in English
    """

    # Get the cursor
    c = conn.cursor()

    # Get the species name
    name = name.strip()
    name = name.replace(',', "")

    c.execute('''
    SELECT * FROM cdpnq_vertebrates
    WHERE name = ?
    ORDER BY rank
    ''', (name,))

    # Get the first result
    result = c.fetchone()

    # Close the cursor
    c.close()

    # Return the result
    if result:
        return {
            'name': result[0],
            'valid_name': result[1],
            'rank': result[2],
            'synonym': result[3],
            'author': result[4],
            'canonical_full': result[5],
            'vernacular_fr': result[6],
            'vernacular_en': result[7]
        }
    else:
        return None

def match_taxa(name) -> dict:
    """Match a species name to the CDPNQ database
    Parameters
    ----------
    name : str

    Returns
    -------
    dict
        A dictionary with the following keys
        name: scientific name
        valid_name: valid scientific name
        rank: rank of the taxa
        synonym: boolean indicating if the name is a synonym
        author: author of the scientific name
        canonical_full: canonical full name
        vernacular_fr: vernacular name in French
        vernacular_fr2: vernacular name in French from Natureserve
        vernacular_en: vernacular name in English
    """

    out = []

    odonates = match_taxa_odonates(name)
    if odonates:
        # Add vernacular_en
        odonates = {**odonates, 'vernacular_en': None}
        out = [*out, odonates]
    
    vertebrates = match_taxa_vertebrates(name)
    if vertebrates:
        # Add vernacular_fr2
        vertebrates = {**vertebrates, 'vernacular_fr2': None}
        out = [*out, vertebrates]
    
    if out:
        return out
    else:
        return None
