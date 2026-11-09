# Look up VASCAN taxa in the custom_source sqlite database

# The vascan_remap table lists the VASCAN taxa that are remapped to their only
# infra-taxon present in Quebec. Its columns are described in custom_sources.txt.
# Rows are looked up with VASCAN taxon IDs, which are the record IDs returned by
# Global Names for VASCAN (data source 147).


import sqlite3
import importlib.resources
import os.path
from typing import List, Union


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


def match_taxa(taxon_id: str) -> Union[dict, None]:
    """Find the remapping of an accepted VASCAN taxon

    Parameters
    ----------
    taxon_id : str
        VASCAN taxonID of an accepted taxon

    Returns
    -------
    dict
        The vascan_remap row of the taxon, None if the taxon is not remapped
    """
    c = conn.cursor()
    c.row_factory = sqlite3.Row
    c.execute('SELECT * FROM vascan_remap WHERE taxon_id = ?', (str(taxon_id),))
    result = c.fetchone()
    c.close()

    return dict(result) if result else None


def get_intermediate_taxa(remap: dict) -> List[dict]:
    """Find the taxa between a remapped taxon and its infra-taxon

    Goes down from the remapped taxon. Every taxon between the two is itself
    remapped to the same infra-taxon, so it has its own row in the table.
    ie. for Cakile edentula (9885), remapped to Cakile edentula edentula edentula
    (3935), returns the row of Cakile edentula edentula (3936).

    Parameters
    ----------
    remap : dict
        A vascan_remap row, as returned by match_taxa

    Returns
    -------
    list of dict
        The vascan_remap rows of the intermediate taxa, highest rank first.
        Empty if the infra-taxon is a direct child of the remapped taxon.
    """
    out = []

    c = conn.cursor()
    c.row_factory = sqlite3.Row
    parent_id = remap['taxon_id']
    while True:
        c.execute(
            'SELECT * FROM vascan_remap WHERE parent_id = ? AND valid_taxon_id = ?',
            (parent_id, remap['valid_taxon_id']))
        child = c.fetchone()
        if child is None:
            break
        out.append(dict(child))
        parent_id = child['taxon_id']
    c.close()

    return out
