import json
from urllib.request import Request, urlopen, URLError, HTTPError
from urllib.parse import urlencode

from datetime import datetime
import multiprocessing
from functools import partial

HOST = "https://explorer.natureserve.org/api"

CURRENT_DATE = datetime.now()

# Metadata is generated from Terms of Use and Citations to reflect DublinCore metadata structure
# https://explorer.natureserve.org/AboutTheData/UseGuidelinesCitations
METADATA = {
    "title": "NatureServe Explorer API",
    "description": "Provides access to NatureServe's biodiversity data, including taxon record retrieval and species search.",
    "creator": "NatureServe",
    "publisher": "NatureServe",
    "date": CURRENT_DATE.strftime("%Y-%m-%d"),
    "termsOfUse": "https://explorer.natureserve.org/AboutTheData/UseGuidelinesCitations",
    "logo": "https://www.natureserve.org/sites/default/files/2022-07/network_logo_250.jpg",
    "rights": "Use governed by NatureServe’s Use Guidelines and Citations.",
    "citation": f"NatureServe. {CURRENT_DATE.year}. NatureServe Network Biodiversity Location Data accessed through NatureServe Explorer. NatureServe, Arlington, Virginia. Available https://explorer.natureserve.org/. (Accessed: {CURRENT_DATE.strftime('%B %d, %Y')}).",
    "licenseURL": "https://explorer.natureserve.org/AboutTheData/UseGuidelinesCitations",
    "creator": "NatureServe",
    "creatorURL": "https://explorer.natureserve.org/",
    "contact": "DataSupport@natureserve.org",
    "resourceType": "api",
    "resourceURL": "https://explorer.natureserve.org/api",
    "resourceName": "NatureServe Explorer API",
    "resourceDescription": "NatureServe Explorer API provides access to NatureServe's biodiversity data.",
}

MATCH_AGAINST = [
    "scientificName",
    "allScientificNames",
    "primaryCommonName",
    "allCommonNames",
    "allNames",
    "code"
]

SIMILARITY_OPERATORS = [
    "similarTo",
    "contains",
    "startsWith",
    "equals"
]

NATIONS = [
    "CA",
    "US",
]

SUBNATIONS = [
    "AB", "BC", "LB", "MB", "NB", "NF", "NS", "NT", "ON", "PE", "QC", "SK", "YT",
]


def _get_url_data(url, method="GET", params=None, body=None):
    """
    Internal helper using urllib to GET or POST JSON.
    """
    # append query params for GET
    if params:
        url = f"{url}?{urlencode(params)}"
    data = None
    headers = {"Content-Type": "application/json"}
    if method.upper() == "POST" and body is not None:
        data = json.dumps(body).encode("utf-8")
    req = Request(url, data=data, headers=headers)
    try:
        resp = urlopen(req, timeout=10)
    except HTTPError as e:
        return {"error": str(e)}
    except URLError as e:
        return {"error": str(e)}
    else:
        try:
            return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return {}


def get_taxon(global_id):
    """
    Fetch a taxon record by its global ID from NatureServe.

    Api reference:
    https://explorer.natureserve.org/api-docs/#_get_taxon

    Parameters
    ----------
    global_id : str
        The NatureServe taxon global identifier (e.g., 'ELEMENT_GLOBAL.2.160636').

    Returns
    -------
    dict
        JSON response as a dictionary.
    """
    url = f"{HOST}/data/taxon/{global_id}"
    return _get_url_data(url, method="GET")


def search_species(species_search_token=None,
                   text_search_operator ="similarTo",
                   text_match_against="allScientificNames",
                   nation=None, subnation=None,
                   page=None, records_per_page=None):
    """
    SearchSpecies via POST with criteriaType, textCriteria, and pagingOptions.

    Api reference:
    https://explorer.natureserve.org/api-docs/#_species_search

    Parameters
    ----------
    search_token : str
        Token to match against all scientific names (primary and synonyms)
        using a “similarTo” operator.
    operator : str, optional
        Similarity operator to use for the search. Default is "similarTo".
        Other options include "contains", "startsWith", and "equals".
    match_against : str, optional
        Field to match against. Default is "allScientificNames".
        Other options include "scientificName", "primaryCommonName",
        "allCommonNames", "allNames", and "code".
    page : int, optional
        Page number for pagination.
    records_per_page : int, optional
        Number of records per page for pagination.

    Returns
    -------
    dict
        JSON response containing search metadata and results under 'data' key.
    """
    if text_search_operator not in SIMILARITY_OPERATORS:
        raise ValueError(f"Invalid operator: {text_search_operator}. Must be one of {SIMILARITY_OPERATORS}.")
    
    if text_match_against not in MATCH_AGAINST:
        raise ValueError(f"Invalid match_against: {text_match_against}. Must be one of {MATCH_AGAINST}.")

    url = f"{HOST}/data/speciesSearch"

    text_criteria = None
    if species_search_token:
        text_criteria = {
            "paramType": "textSearch",
            "searchToken": species_search_token,
            "operator": text_search_operator,
            "matchAgainst": text_match_against
        }

    location_criteria = None

    if nation and nation in NATIONS:
        location_criteria = {
            "paramType": "nation",
            "nation": nation
        }

    if subnation and not nation:
        raise ValueError("Subnation cannot be specified without a nation.")
    elif subnation and subnation in SUBNATIONS:
        location_criteria = {
            "paramType": "subnation",
            "subnation": subnation,
            "nation": nation
        }

    body = {
        "criteriaType": "species",
        "pagingOptions": {
            "page": page,
            "recordsPerPage": records_per_page
        }
    }

    body.update({"textCriteria": [text_criteria]}) if text_criteria else None

    body.update({"locationCriteria": [location_criteria]}) if location_criteria else None

    result = _get_url_data(url, method="POST", body=body)
    if not isinstance(result, dict) or 'results' not in result:
        return None
    return result

def _search_page(args):
    """Helper function for multiprocessing that searches a specific page"""
    page, search_func = args
    return search_func(page=page)

def search_all_species(records_per_page=100, num_processes=4, **kwargs):
    """
    Search all species by paginating through results using parallel processing.
    
    Parameters
    ----------
    records_per_page : int, optional
        Number of records per page. Default value is 100. Maximum value is 100.
    num_processes : int, optional
        Number of processes to use for parallel processing. Default is 4.
    **kwargs : 
        Additional arguments to pass to search_species function.
        
    Returns
    -------
    list
        Combined results from all pages.
    """
    if records_per_page > 100:
        raise ValueError("records_per_page must be less than or equal to 100.")

    # Get first page to determine total pages
    first_page = search_species(page=0, records_per_page=records_per_page, **kwargs)
    if not first_page or 'resultsSummary' not in first_page:
        return []
    
    try:
        total_pages = int(first_page['resultsSummary']['totalPages'])
        all_results = first_page.get('results', [])
    except (KeyError, ValueError):
        return first_page.get('results', [])
    
    if total_pages <= 1:
        return all_results
    
    # Create a partial function with fixed parameters
    search_func = partial(search_species, records_per_page=records_per_page, **kwargs)
    
    # Create a list of page numbers to process (skip page 0 as we already processed it)
    pages_to_process = list(range(1, total_pages))
    
    # Process pages in parallel
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Map page numbers to search_func with the helper function
        args = [(page, search_func) for page in pages_to_process]
        results = pool.map(_search_page, args)
    
    # Combine all results
    for result in results:
        if result and isinstance(result, dict) and 'results' in result:
            all_results.extend(result['results'])
    
    return all_results