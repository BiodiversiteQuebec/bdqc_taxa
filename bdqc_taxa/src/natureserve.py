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

LOCATION_NATIONS = [
    "CA",
    "US",
]

LOCATION_SUBNATIONS = [
    "AB", "BC", "LB", "MB", "NB", "NF", "NS", "NT", "ON", "PE", "QC", "SK", "YT",
]

LOCATION_ORIGIN = [
    "all",
    "onlyNatives",
    "onlyExotics"
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


# Optional; Defines whether searches will be limited to only include species which are native or exotic within the specified locations. Defaults to "all" if the property or the locationOptions object is not specified. Only applicable when searching for species. If specified for a search which does not return species, this property will be ignored. Possible values: all, onlyNatives, onlyExotics

def search_species(species_search_token=None,
                   text_search_operator ="similarTo",
                   text_match_against="allScientificNames",
                   location_nation=None, location_subnation=None,
                   location_origin=None,
                   page=None, records_per_page=None,
                   additional_criterias=None):
    """
    SearchSpecies via POST with criteriaType, textCriteria, and pagingOptions.

    Api reference:
    https://explorer.natureserve.org/api-docs/#_species_search

    Parameters
    ----------
    text_search_token : str
        Token to match against all scientific names (primary and synonyms)
        using a “similarTo” operator.
    text_search_operator : str, optional
        Similarity operator to use for the search. Default is "similarTo".
        Other options include "contains", "startsWith", and "equals".
    text_match_against : str, optional
        Field to match against. Default is "allScientificNames".
        Other options include "scientificName", "primaryCommonName",
        "allCommonNames", "allNames", and "code".
    location_nation : str, optional
        Nation code to limit the search to a specific nation.
        Must be one of "CA" or "US".
    location_subnation : str, optional
        Subnation code to limit the search to a specific subnation.
        Must be one of the subnation codes for Canada. Example: "ON", "PE", "QC", ...
    location_origin : str, optional
        Origin of the species. Must be one of "all", "onlyNatives", or "onlyExotics".   
    page : int, optional
        Page number for pagination.
    records_per_page : int, optional
        Number of records per page for pagination.
    additional_criterias : dict, optional
        Additional criteria to include in the search (statusCriteria, etc).

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

    if location_nation and location_nation in LOCATION_NATIONS:
        location_criteria = {
            "paramType": "nation",
            "nation": location_nation
        }

    if location_subnation and not location_nation:
        raise ValueError("Subnation cannot be specified without a nation.")
    elif location_subnation and location_subnation in LOCATION_SUBNATIONS:
        location_criteria = {
            "paramType": "subnation",
            "subnation": location_subnation,
            "nation": location_nation
        }

    location_options = None
    if location_origin and not location_nation:
        raise ValueError("Origin cannot be specified without a nation.")
    elif location_origin and location_origin in LOCATION_ORIGIN:
        location_options = {
            "origin": location_origin
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

    body.update({"locationOptions": location_options}) if location_options else None

    body.update({"additionalCriteria": additional_criterias}) if additional_criterias else None

    result = _get_url_data(url, method="POST", body=body)
    if not isinstance(result, dict) or 'results' not in result:
        return None
    return result

def _search_page(args):
    """Helper function for multiprocessing that searches a specific page"""
    page, search_func = args
    return search_func(page=page)

def search_all_species(records_per_page=100, num_processes=4, max_records=None, **kwargs):
    """
    Search all species by paginating through results using parallel processing.
    
    Parameters
    ----------
    records_per_page : int, optional
        Number of records per page. Default value is 100. Maximum value is 100.
    num_processes : int, optional
        Number of processes to use for parallel processing. Default is 4.
    max_records : int, optional
        Maximum number of records to return. If None, returns all records.
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
    
    if total_pages <= 1 or (max_records and len(all_results) >= max_records):
        return all_results[:max_records] if max_records else all_results
    
    # Adjust total pages if max_records is specified
    if max_records:
        remaining_records = max_records - len(all_results)
        pages_needed = min(total_pages - 1, (remaining_records + records_per_page - 1) // records_per_page)
        pages_to_process = list(range(1, pages_needed + 1))
    else:
        pages_to_process = list(range(1, total_pages))
    
    # Create a partial function with fixed parameters
    search_func = partial(search_species, records_per_page=records_per_page, **kwargs)
    
    # Process pages in parallel
    with multiprocessing.Pool(processes=min(num_processes, len(pages_to_process))) as pool:
        # Map page numbers to search_func with the helper function
        args = [(page, search_func) for page in pages_to_process]
        results = pool.map(_search_page, args)
    
    # Combine all results
    for result in results:
        if result and isinstance(result, dict) and 'results' in result:
            all_results.extend(result['results'])
            if max_records and len(all_results) >= max_records:
                return all_results[:max_records]
    
    return all_results