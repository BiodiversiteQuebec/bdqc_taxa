from urllib.request import Request, urlopen, URLError, HTTPError
from urllib.parse import urlencode
import json
from inspect import signature
from .cache import cache


HOST = "https://api.gbif.org"
LIMIT = 100
RESP_RESULT_KEY = 'results'
GBIF_TAXONOMIC_BACKBONE_DATASET_KEY = 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'


@cache.memoize()
def _get_url_data(url, params: dict = None, limit: int = None, offset: int = 0):
    if not params:
        params = {}
    if limit:
        params.update({
            "limit": limit,
            "offset": offset
        })
    req = Request(
        url=f"{url}?{urlencode(params)}",
        headers={"Content-Type": "application/json"})
    try:
        data = urlopen(req)
    except HTTPError as e:
        return e
    except URLError as e:
        if hasattr(e, 'reason'):
            return e.reason
        elif hasattr(e, 'code'):
            return e.code
        else:
            return e
    else:
        try:
            out = json.loads(data.read().decode('utf-8'))
            return out
        except KeyError:
            return [None]

def _pagin_get_url_data(url, params: dict = None, limit: int = LIMIT,
    resp_result_key = RESP_RESULT_KEY):

    end_of_records = False
    offset = 0
    results = []
    while not end_of_records:
        resp = _get_url_data(url, params, limit = LIMIT, offset = offset)
        end_of_records = resp["endOfRecords"]
        offset += limit
        results.extend(resp[resp_result_key])
    return results


class Species:
    @classmethod
    def get_vernacular_name(cls, species_id: int):
        url = f"{HOST}/v1/species/{species_id}/vernacularNames"
        results = _pagin_get_url_data(url)
        return results

    @classmethod
    def match_v1(cls, name: str = "", rank: str = "", strict: str = "", 
        verbose: str = "", kingdom: str = "", phylum: str = "",
        sp_class: str = "", order: str = "", family: str = "",
        genus: str = ""):

        import warnings
        warnings.warn("Version 1 - Warning: this method will be removed and users are advised to migrate to the version 2 API v2/species/match supported by the Species.match method.", DeprecationWarning, stacklevel=2)

        arg_keys = signature(cls.match).parameters
        arg_values = locals()
        params = {
            "name": name,
            "rank": rank,
            "strict": strict,
            "verbose": verbose,
            "kingdom": kingdom,
            "phylum": phylum,
            "class": sp_class,
            "order": order,
            "family": family,
            "genus": genus
        }
        url = f"{HOST}/v1/species/match"
        results = _get_url_data(url, params)
        return results

    @classmethod
    def match_v2(cls, scientific_name: str = "", taxon_rank: str = "", strict: str = "", 
        verbose: str = "", kingdom: str = "", phylum: str = "",
        sp_class: str = "", order: str = "", family: str = "",
        genus: str = ""):

        params = {
            "scientificName": scientific_name,
            "taxonRank": taxon_rank,
            "strict": strict,
            "verbose": verbose,
            "kingdom": kingdom,
            "phylum": phylum,
            "class": sp_class,
            "order": order,
            "family": family,
            "genus": genus
        }
        url = f"{HOST}/v2/species/match"
        results = _get_url_data(url, params)
        return results
    
    @classmethod
    def match(cls, scientific_name: str = "", taxon_rank: str = "", **kwargs):
        """
        Match a scientific name to a GBIF species record.
        """
        if not scientific_name:
            raise ValueError("scientific_name must be provided")
        
        
        # Workaround when returned record is `HIGHERRANK` of rank = `kingdom`
        # example {"usage":{"key":"1","name":"Animalia","canonicalName":"Animalia","rank":"KINGDOM","status":"ACCEPTED","type":"SCIENTIFIC","formattedName":"<i>Animalia</i>"},"classification":[{"key":"1","name":"Animalia","rank":"KINGDOM"}],"diagnostics":{"matchType":"HIGHERRANK","confidence":95,"timeTaken":38,"timings":{"nameNRank":0,"sciNameMatch":39,"nameParse":0,"luceneMatch":39}},"synonym":false,"left":597323,"right":891973}

        out = cls.match_v2(scientific_name=scientific_name, taxon_rank=taxon_rank, **kwargs)
        rank = out.get('usage', {}).get('rank', '').upper()
        match_type = out.get('diagnostics', {}).get('matchType', '')
        if rank == 'KINGDOM' and match_type == 'HIGHERRANK':
            # re-attempt with verbose='true' to get [diagnostic][alternatives][0]
            out_2 = cls.match_v2(scientific_name=scientific_name, taxon_rank=taxon_rank, verbose='true', **kwargs)
            alternatives = out_2.get('diagnostics', {}).get('alternatives', [])
            out = alternatives[0] if alternatives else out

        return out
    
    @classmethod
    def search(cls, query: str = "", dataset_key: str = GBIF_TAXONOMIC_BACKBONE_DATASET_KEY,
               **kwargs):
        
        params = {
            "q": query,
            "datasetKey": dataset_key
        }

        params.update(kwargs)

        url = f"{HOST}/v1/species/search"
        results = _get_url_data(url, params)
        return results

    @classmethod
    def get(cls, key: int):
        url = f"{HOST}/v1/species/{key}"
        results = _get_url_data(url)
        return results