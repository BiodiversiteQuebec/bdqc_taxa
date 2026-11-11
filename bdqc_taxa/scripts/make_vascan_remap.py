#====================================================================================================
# Create a vascan_sp_remap table from a VASCAN Darwin Core Archive
#
# NOTES
# - A species (or an intermediate infra-taxon, e.g. a subspecies that contains varieties) is remapped
#   to its infra-taxon when exactly one of its lowest-level infra-taxa is present in Quebec.
#   ie. Arnica griscomii (2860) has subsp. griscomii (native in QC) and subsp. frigida (not in QC),
#   so Arnica griscomii is remapped to Arnica griscomii griscomii (2861).
# - Only the remapped taxa are kept. When the target is a variety under a subspecies, the
#   intermediate subspecies is remapped too, so it has its own row:
#   ie. Cakile edentula (9885) and Cakile edentula edentula (3936) are both remapped to
#   Cakile edentula edentula edentula (3935). The full hierarchy of a target is rebuilt in taxa_ref.py
#   by following parent_id between the rows that share its valid_taxon_id.
# - The archive was downloaded from https://data.canadensys.net/vascan/checklist with the criteria:
#     Region: Quebec only
#     Status: native, introduced, ephemeral, doubtful, extirpated, excluded
#     Ranks: species, subspecies, variety (hybrids included)
# - Infra-taxa absent from Quebec are not in such an archive, which is fine: only infra-taxa
#   listed for Quebec can be counted. Excluded infra-taxa are listed, so the Quebec status of
#   each infra-taxon is still needed to know which ones are counted.
# - Rows are matched on VASCAN taxon IDs, which are the record IDs returned by Global Names
#   for VASCAN (data source 147).
#====================================================================================================

# %%
import pandas as pd
import sqlite3
import re
import os

# Set the path to the file directory
file_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(file_dir)

DB_FILE = "../custom_sources.sqlite"
README_FILE = "../custom_sources.txt"
DWCA_PATH = "../../scratch/vascan_dwc_03-11-2026/"
CSV_EXPORT = "../../scratch/vascan_remap.csv"
TABLE_NAME = "vascan_remap"

QC_LOCATION_ID = "ISO 3166-2:CA-QC"

# The archive uses Darwin Core values for occurrenceStatus, VASCAN uses other labels on its website.
# Mapping verified against the VASCAN API on 2026-11-04.
#   (occurrenceStatus, establishmentMeans) -> VASCAN status
VASCAN_STATUS = {
    ("present", "native"): "native",
    ("present", "introduced"): "introduced",
    ("irregular", "introduced"): "ephemeral",
    ("doubtful", ""): "doubtful",
    ("absent", ""): "extirpated",
    ("excluded", ""): "excluded",
}

# Statuses counted as "present in Quebec" for the remap rule.
# A wrong remap is worse than a missed one, so unclear statuses are counted:
# counting an extra infra-taxon prevents a remap rather than causing one.
#   - doubtful: reported but not validated
#   - extirpated: gone today, but historical observations may be of this taxon
#   - excluded: erroneous or not established, so not counted
COUNTED_STATUSES = {"native", "introduced", "ephemeral", "doubtful", "extirpated"}

# %% Load the data
taxa = pd.read_csv(os.path.join(DWCA_PATH, "taxon.txt"), sep="\t", dtype=str, quoting=3, keep_default_na=False)
distribution = pd.read_csv(os.path.join(DWCA_PATH, "distribution.txt"), sep="\t", dtype=str, quoting=3, keep_default_na=False)

# Keep only the accepted taxa, assumes that matching through
# GlobalNamesResolver took care of synonyms
accepted = taxa[taxa["taxonomicStatus"] == "accepted"].copy()
accepted_by_id = accepted.set_index("taxonID")

# Get the date the archive was generated, written by VASCAN in bibliographicCitation
consulted = (
    taxa["bibliographicCitation"]
    .str.extract(r"consulted on (\d{4}-\d{2}-\d{2})")[0]
    .dropna()
    .mode()
    .iloc[0]
)

# %% Build the canonical names

# Names are built without authorship nor rank marker and follow the parent chain, the same way
# taxa_ref.py stores VASCAN names from the Global Names classification path, the rank being
# stored separately. ie. 'Arnica griscomii griscomii' (subspecies),
# 'Cakile edentula edentula edentula' (variety of Cakile edentula subsp. edentula)
canonical_names = {}

def get_canonical_name(taxon_id):
    if taxon_id in canonical_names:
        return canonical_names[taxon_id]
    row = accepted_by_id.loc[taxon_id]
    if row["taxonRank"] == "species":
        name = f"{row['genus']} {row['specificEpithet']}"
    else:
        parent_name = get_canonical_name(row["parentNameUsageID"])
        name = f"{parent_name} {row['infraspecificEpithet']}"
    canonical_names[taxon_id] = name
    return name

for taxon_id in accepted_by_id.index:
    get_canonical_name(taxon_id)

# %% Get the Quebec status of each infra-taxon

qc = distribution[distribution["locationID"] == QC_LOCATION_ID].copy()

# Stop if the archive contains a status combination that is not mapped
status_keys = list(zip(qc["occurrenceStatus"], qc["establishmentMeans"]))
unknown_status = set(status_keys) - set(VASCAN_STATUS)
if unknown_status:
    raise ValueError(f"Unknown Quebec status combinations in the archive: {unknown_status}")

if qc["taxonID"].duplicated().any():
    raise ValueError("Some taxa have more than one Quebec distribution row")

qc["vascan_status"] = [VASCAN_STATUS[k] for k in status_keys]
qc_status = qc.set_index("taxonID")["vascan_status"]

# %% Find the taxa to remap

# Accepted children of each taxon
children = accepted.groupby("parentNameUsageID")["taxonID"].apply(list).to_dict()

# Lowest-level infra-taxa under a taxon (ie. the varieties of Cakile edentula edentula)
def get_leaves(taxon_id):
    kids = children.get(taxon_id, [])
    if not kids:
        return [taxon_id]
    return [leaf for kid in kids for leaf in get_leaves(kid)]

# Species and intermediate subspecies that have accepted infra-taxa below them
candidates = [
    taxon_id for taxon_id in accepted_by_id.index
    if taxon_id in children and accepted_by_id.loc[taxon_id, "taxonRank"] in ("species", "subspecies")
]

remap_rows = []
for taxon_id in candidates:
    if taxon_id in qc_status.index:
        print(f"Warning: {canonical_names[taxon_id]} ({taxon_id}) has children and its own Quebec status")

    leaves = get_leaves(taxon_id)
    counted = [leaf for leaf in leaves if qc_status.get(leaf) in COUNTED_STATUSES]

    # Remap only when exactly one infra-taxon is present in Quebec
    if len(counted) != 1:
        continue

    valid_id = counted[0]

    # Parent inside the archive only (the archive has no genus), so species have no parent
    parent_id = accepted_by_id.loc[taxon_id, "parentNameUsageID"]
    if parent_id not in accepted_by_id.index:
        parent_id = None

    remap_rows.append({
        "taxon_id": taxon_id,
        "name": canonical_names[taxon_id],
        "rank": accepted_by_id.loc[taxon_id, "taxonRank"],
        "parent_id": parent_id,
        "valid_taxon_id": valid_id,
        "valid_name": canonical_names[valid_id],
        "valid_rank": accepted_by_id.loc[valid_id, "taxonRank"],
        "valid_authorship": accepted_by_id.loc[valid_id, "scientificNameAuthorship"],
        "valid_qc_status": qc_status[valid_id],
        "n_infra_listed_qc": len(leaves),
        "n_infra_counted_qc": len(counted),
        "vascan_consulted": consulted,
    })

remap = pd.DataFrame(remap_rows)

# %% Add the manual remaps

# Taxa missing from the archive because VASCAN has no Quebec distribution for them, while the
# CDPNQ lists the infra-taxon in Quebec, and it is the only infra-taxon of the species in VASCAN.
#  ie. Cirsium scariosum var. scariosum (Menacée, chardon de la Minganie) is only listed in AB and BC.
#  Verified on the VASCAN website on 2026-11-11.
MANUAL_REMAPS = [{
    "taxon_id": "3004",
    "name": "Cirsium scariosum",
    "rank": "species",
    "parent_id": None,
    "valid_taxon_id": "3003",
    "valid_name": "Cirsium scariosum scariosum",
    "valid_rank": "variety",
    "valid_authorship": "Nuttall",
    "valid_qc_status": None,
    "n_infra_listed_qc": 0,
    "n_infra_counted_qc": 0,
    "vascan_consulted": consulted,
}]

manual_ids = {row["taxon_id"] for row in MANUAL_REMAPS}
if manual_ids & set(remap["taxon_id"]):
    raise ValueError(f"Manual remaps already in the archive, remove them: {manual_ids & set(remap['taxon_id'])}")

remap = pd.concat([remap, pd.DataFrame(MANUAL_REMAPS)], ignore_index=True)
remap = remap.sort_values("name").reset_index(drop=True)

print(f"Accepted taxa: {len(accepted)} | taxa with a Quebec status: {len(qc_status)}")
print(f"Rows in {TABLE_NAME}: {len(remap)} ({remap['rank'].value_counts().to_dict()})")

# # Verification
# remap.loc[remap['name'] == 'Arnica griscomii', :]
# remap.loc[remap['valid_name'] == 'Cakile edentula edentula edentula', :]
# remap.loc[remap['valid_qc_status'] != 'native', :]

# %% Export to csv

remap.to_csv(CSV_EXPORT, index=False)

# %% Write to sqlite database

conn = sqlite3.connect(DB_FILE)

# Drop the table if it exists
conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

# Write the table
remap.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

# Index on the VASCAN taxon ID used for matching
conn.execute(f"CREATE UNIQUE INDEX {TABLE_NAME}_taxon_id_idx ON {TABLE_NAME} (taxon_id)")
conn.commit()
conn.close()

# %% Append to the sqlite README file

readme = f"""TABLE {TABLE_NAME}

Description:
    This file was generated from the VASCAN Darwin Core Archive {os.path.basename(os.path.normpath(DWCA_PATH))}.
    The archive was obtained from https://data.canadensys.net/vascan/checklist on {consulted}
    (region: Quebec; statuses: native, introduced, ephemeral, doubtful, extirpated, excluded;
    ranks: species, subspecies, variety, hybrids included).
    The file was parsed using the script `scripts/make_vascan_remap.py`.
    Species (and intermediate infra-taxa) that have exactly one infra-taxon present in Quebec
    are mapped to that infra-taxon. All Quebec observations of the species are assumed to be the infra-taxon.
    Only the remapped taxa are listed.
    Manual remaps are added for taxa with no Quebec distribution in VASCAN, while the CDPNQ lists the
    infra-taxon in Quebec and it is the only infra-taxon of the species in VASCAN
    (Cirsium scariosum -> Cirsium scariosum scariosum). Their n_infra_* columns are 0.
    Infra-taxa counted as present in Quebec: {', '.join(sorted(COUNTED_STATUSES))}.

Columns:
    taxon_id: VASCAN taxonID of the taxon to remap (species or intermediate subspecies)
    name: canonical name of the taxon to remap, without authorship nor rank marker (ie. Cakile edentula edentula)
    rank: rank of the taxon to remap
    parent_id: VASCAN taxonID of the parent of the taxon to remap, empty for species
    valid_taxon_id: VASCAN taxonID of the infra-taxon to remap to
    valid_name: canonical name of the infra-taxon, without authorship nor rank marker (ie. Arnica griscomii griscomii)
    valid_rank: rank of the infra-taxon (subspecies or variety)
    valid_authorship: authorship of the infra-taxon
    valid_qc_status: VASCAN status of the infra-taxon in Quebec
    n_infra_listed_qc: number of lowest-level infra-taxa listed for Quebec, any status
    n_infra_counted_qc: number of those counted as present in Quebec (always 1, 0 for manual remaps)
    vascan_consulted: date the VASCAN archive was generated

Notes:
    Match on VASCAN taxon IDs, which are the record IDs returned by Global Names for VASCAN (data source 147).
    The hierarchy of an infra-taxon is rebuilt by following parent_id between the rows that share its
    valid_taxon_id (ie. 3935 <- 3936 <- 9885 for Cakile edentula edentula edentula).
"""

# Replace the section if it already exists, otherwise append it
with open(README_FILE, encoding="utf-8") as f:
    content = f.read()

section_pattern = re.compile(rf"TABLE {TABLE_NAME}\n.*?(?=\nTABLE |\Z)", re.DOTALL)
if section_pattern.search(content):
    content = section_pattern.sub(lambda _: readme, content)
else:
    content = content.rstrip("\n") + "\n\n\n" + readme

with open(README_FILE, "w", encoding="utf-8") as f:
    f.write(content)

# %%
