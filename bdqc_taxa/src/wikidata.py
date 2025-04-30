import urllib.request
import json
from typing import Union, List, Optional

BASE_URL = "https://www.wikidata.org/w/api.php?"

TAXA_RANKS_QID = {'domain': 'Q146481', 'kingdom':'Q36732', 'subkingdom': 'Q2752679', 'infrakingdom': 'Q3150876', 'phylum': 'Q38348', 'subphylum': 'Q1153785', 'infraphylum': 'Q2361851', 'superclass': 'Q3504061', 'class': 'Q37517', 'subclass': 'Q5867051', 'infraclass': 'Q2007442', 'superorder': 'Q5868144', 'order': 'Q36602', 'suborder': 'Q5867959', 'infraorder': 'Q2889003', 'superfamily': 'Q2136103', 'family': 'Q35409', 'subfamily': 'Q164280', 'tribe': 'Q227936', 'subtribe': 'Q3965313', 'genus': 'Q34740', 'subgenus': 'Q3238261', 'species': 'Q7432', 'subspecies': 'Q68947', 'variety': 'Q767728', 'subvariety': 'Q630771', 'form': 'Q279749', 'subform': 'Q12774043'}
# TAXA_RANKS_QID obtained from _get_taxa_rank_entities()

def search_entities(query, language="en", rank:Optional[str] = None) -> dict:
    """
    Search for entities on Wikidata based on a query.

    Args:
    - query (str): The search term.
    - language (str, optional): The language to search in. Default is English ('en').

    Returns:
    - dict: Search results as a dictionary.
    """
    
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": language,
        "format": "json",
        "type": "item",
        "limit": 50
    }

    url = BASE_URL + urllib.parse.urlencode(params)
    response = urllib.request.urlopen(url).read()
    data = json.loads(response)

    # Raise an exception if the request was not successful
    if "error" in data:
        raise Exception(data["error"]["info"])
    
    # Return search results
    results = data["search"]
    
    return results

def get_entities(id: Union[str, List[str]], languages=["en, fr"]):
    """
    Get details of a specific entity from Wikidata based on its QID.

    Args:
    - id (Union[str, List[str]]): The identifier(s) of the entity (e.g., Q12345 or a list of QIDs).
    - languages (list, optional): List of languages to fetch details in. Default is ['en', 'fr'].

    Returns:
    - dict: Entity details as a dictionary.
    """
    if isinstance(id, list):
        id = "|".join(id)
    
    params = {
        "action": "wbgetentities",
        "ids": id,
        "languages": "|".join(languages),
        "format": "json"
    }
    
    url = BASE_URL + urllib.parse.urlencode(params)
    response = urllib.request.urlopen(url).read()
    data = json.loads(response)

    # Raise an exception if the request was not successful
    if "error" in data:
        raise Exception(data["error"]["info"])
    
    # Return entity details

    entities = data["entities"].values()

    return list(entities)


def _get_taxa_rank_entities() -> dict:
    """
    Get the QID of a taxon rank entities based on the rank name.

    Returns:
    - dict: A dictionary with the rank name as key and the QID as value.
    """
    TAXONOMIC_RANKS = [
        "Domain",
        "Kingdom",
        "Subkingdom",
        "Infrakingdom",
        "Phylum",
        "Subphylum",
        "Infraphylum",
        "Superclass",
        "Class",
        "Subclass",
        "Infraclass",
        "Superorder",
        "Order",
        "Suborder",
        "Infraorder",
        "Superfamily",
        "Family",
        "Subfamily",
        "Tribe",
        "Subtribe",
        "Genus",
        "Subgenus",
        "Species",
        "Subspecies",
        "Variety",
        "Subvariety",
        "Form",
        "Subform"
    ]
    TAXONOMIC_RANKS = [rank.lower() for rank in TAXONOMIC_RANKS]

    RANKS_CAT_QIDS = ['Q13578154', 'Q3100180', 'Q427626']

    out = {}

    for rank in TAXONOMIC_RANKS:
        # Search for the taxon rank entity
        search_results = search_entities(rank)

        qids = [result["id"] for result in search_results]

        # Get the entity details
        entities = get_entities(qids, languages=["en"])

        try:
            entity = next(entity for entity in entities if 'P31' in entity['claims'] and any(entity['claims']['P31'][0]['mainsnak']['datavalue']['value']['id'] == qid for qid in RANKS_CAT_QIDS))
        except StopIteration:
            continue
        out[rank] = entity["id"]
    
    return out

