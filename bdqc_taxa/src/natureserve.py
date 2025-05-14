import json
from urllib.request import Request, urlopen, URLError, HTTPError
from urllib.parse import urlencode

HOST = "https://explorer.natureserve.org/api"


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


def search_species(search_token, page=None, records_per_page=None):
    """
    SearchSpecies via POST with criteriaType, textCriteria, and pagingOptions.

    Api reference:
    https://explorer.natureserve.org/api-docs/#_species_search

    Parameters
    ----------
    search_token : str
        Token to match against all scientific names (primary and synonyms)
        using a “similarTo” operator.
    page : int, optional
        Page number for pagination.
    records_per_page : int, optional
        Number of records per page for pagination.

    Returns
    -------
    dict
        JSON response containing search metadata and results under 'data' key.
    """
    url = f"{HOST}/data/speciesSearch"
    body = {
        "criteriaType": "species",
        "textCriteria": [
            {
                "paramType": "textSearch",
                "searchToken": search_token,
                "matchAgainst": "allScientificNames",
                "operator": "similarTo"
            }
        ],
        "pagingOptions": {
            "page": page,
            "recordsPerPage": records_per_page
        }
    }
    result = _get_url_data(url, method="POST", body=body)
    if not isinstance(result, dict) or 'results' not in result:
        return []
    return result["results"]