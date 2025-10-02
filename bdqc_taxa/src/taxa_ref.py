from __future__ import annotations
from os import readlink
from . import global_names
from . import gbif
from . import bryoquel
from . import cdpnq
from typing import List, Optional
from inspect import signature

GBIF_SOURCE_KEY = 11 # Corresponds to global names
BRYOQUEL_SOURCE_KEY = 1001 # Not in global names so start at 1000
BROQUEL_SOURCE_NAME = 'Bryoquel'
CDPNQ_SOURCE_NAME = 'CDPNQ'
CDPNQ_SOURCE_KEY = 1002 # Not in global names so start at 1000

GBIF_SOURCE_NAME = 'GBIF Backbone Taxonomy'
GBIF_RANKS = ['kingdom', 'phylum', 'class', 'order', 'family',
                'genus', 'species', 'subspecies', 'variety', 'form']

DATA_SOURCES = [1, 3, 147] # COL, ITIS, VASCAN

SOURCES_PARENT_CLASSIFICATION_SRIDS = [
    # Only vascular plants
    {
        'source_name': 'VASCAN',
        'ranks': ['kingdom', 'phylum'],
        'scienfitic_name': ['Plantae', 'Tracheophyta']
    },
    # Only Bryoquel
    {
        'source_name': 'Bryoquel',
        'ranks': ['kingdom', 'phylum'],
        'scienfitic_name': ['Plantae', 'Bryophyta']
    },
    # Only CDPNQ mammals
    {
        'source_name': 'CDPNQ',
        'ranks': ['kingdom', 'phylum', 'class'],
        'scienfitic_name': ['Animalia', 'Chordata', 'Mammalia']
    },
    # Only CDPNQ odonata
    {
        'source_name': 'CDPNQ',
        'ranks': ['kingdom', 'phylum', 'class', 'order'],
        'scienfitic_name': ['Animalia', 'Arthropoda', 'Insecta', 'Odonata']
    }
]

class TaxaRef:
    def __init__(self,
                 scientific_name: str = '',
                 id: int | None = None,
                 source_id: int | None = None,
                 source_record_id: str = '',
                 source_name: str = '',
                 authorship: str = '',
                 rank: str = '',
                 rank_order: int | None = None,
                 classification_srids: List[str] | None = None,
                 valid: bool | None = None,
                 valid_srid: str = '',
                 match_type: str | None = '',
                 is_parent: bool | None = None):
        self.scientific_name = scientific_name
        self.id = id
        self.source_id = source_id
        self.source_record_id = source_record_id
        self.source_name = source_name
        self.authorship = authorship
        self.rank = rank.lower()
        self.rank_order = rank_order
        self.classification_srids = classification_srids
        self.valid = valid
        self.valid_srid = valid_srid
        self.match_type = match_type
        self.is_parent = is_parent

    def __repr__(self):
        return f"{self.__class__.__name__}(\'{self.scientific_name}\')"
    
    def __str__(self):
        return self.scientific_name

    @property
    def __dict__(self):
        return {
            "id": self.id,
            "source_id": self.source_id,
            "source_record_id": self.source_record_id,
            "source_name": self.source_name,
            "scientific_name": self.scientific_name,
            "authorship": self.authorship,
            "rank": self.rank,
            "rank_order": self.rank_order,
            "classification_srids": self.classification_srids,
            "valid": self.valid,
            "valid_srid": self.valid_srid,
            "match_type": self.match_type,
            "is_parent": self.is_parent
        }

    @classmethod
    def from_global_names(cls, name: str, authorship: Optional[str] = None, data_sources: Optional[List[int]] = None):
        if data_sources is None:
            data_sources = DATA_SOURCES

        gn_results = global_names.verify(name, authorship, data_sources=data_sources)
        gn_results = gn_results['names']
        try:
            gn_results = [
                result for species in gn_results 
                for result in species['results']]
        except KeyError:
            return []
        out = []
        for result in gn_results:
            is_valid = result["currentRecordId"] == result["recordId"]
            if not is_valid:
                out_kwargs = {
                    "source_id": result["dataSourceId"],
                    "source_record_id": result["recordId"],
                    "source_name": result["dataSourceTitleShort"],
                    "scientific_name": result["matchedCanonicalFull"],
                    "authorship": find_authorship(
                        result["matchedName"],
                        result["matchedCanonicalFull"]),
                    "rank": result["classificationRanks"].split("|")[-1],
                    "rank_order": result["classificationRanks"].count("|") + 1,
                    "classification_srids":
                        result["classificationIds"].split("|"),
                    "valid": is_valid,
                    "valid_srid": result["currentRecordId"],
                    "match_type": result["matchType"].lower(),
                    "is_parent": True if result["matchType"] in ("PartialExact", "PartialFuzzy") else False
                }
                out.append(cls(**out_kwargs))
            for rank_order, taxa_attributes in enumerate(zip(
                result["classificationPath"].split("|"),
                result["classificationRanks"].split("|"),
                result["classificationIds"].split("|"))
            ):
                taxa, rank, srid = taxa_attributes
                match_type = None
                if rank == result["classificationRanks"].split("|")[-1]:
                    valid_authorship = find_authorship(
                        result["matchedName"],
                        result["matchedCanonicalFull"])
                    is_parent = False
                    if result["matchType"] in ("PartialExact", "PartialFuzzy"):
                        is_parent = True
                    if is_valid:
                        match_type = result["matchType"].lower()
                else:
                    valid_authorship = None
                    is_parent = True
                out_kwargs = {
                        "source_id": result["dataSourceId"],
                        "source_record_id": srid,
                        "source_name": result["dataSourceTitleShort"],
                        "scientific_name": taxa,
                        "authorship": valid_authorship,
                        "rank": rank,
                        "rank_order": rank_order,
                        "classification_srids":
                            result["classificationIds"].split(
                                "|")[:rank_order + 1],
                        "valid": True,
                        "valid_srid": srid,
                        "match_type": match_type,
                        "is_parent": is_parent
                    }
                out.append(cls(**out_kwargs))
        return out

    @classmethod
    def from_gbif(cls, name: str, authorship: str = None):
        out = []
        names = [v.strip() for v in name.split("|")]
        for name in names:
            out.extend(cls._from_gbif_singleton(name, authorship))
        
        return out

    @classmethod
    def _from_gbif_singleton(cls, name: str, authorship: str = None):
        if isinstance(authorship, str) and authorship.strip():
            name =" ".join([name, authorship])
        match_species = gbif.Species.match(name)
        try:
            result: dict = gbif.Species.get(match_species['usageKey'])
        except KeyError:
            return []
        is_valid = "acceptedKey" not in result.keys()

        out = []

        # Compute values for taxa_ref rows
        try:
            result['rank_order'] = GBIF_RANKS.index(result['rank'].lower())
        except ValueError:
            result['rank_order'] = None

        if match_species["matchType"].lower() == "higherrank":
            is_parent = True
        else:
            is_parent = False
        
        # Create row for invalid requested taxon
        if not is_valid:
            out_kwargs = {
                "source_id": GBIF_SOURCE_KEY,
                "source_name": GBIF_SOURCE_NAME,
                "source_record_id": result["key"],
                "scientific_name": result["canonicalName"],
                "authorship": strip_authorship(result['authorship']),
                "rank": result['rank'].lower(),
                "rank_order": [i for i, rank in enumerate(GBIF_RANKS)
                    if rank == result['rank'].lower()][0],
                "classification_srids": [
                    result[f'{k}Key'] for k in GBIF_RANKS if k in result.keys()
                    ],
                "valid": is_valid,
                "valid_srid": result["acceptedKey"],
                "match_type": match_species["matchType"].lower(),
                "is_parent": is_parent
            }
            out.append(cls(**out_kwargs))
            if "acceptedUsageKey" in match_species.keys():
                result = gbif.Species.get(match_species['acceptedUsageKey'])
            else:
                result = gbif.Species.get(result['acceptedKey'])

        # Create rows for valid taxon
        classification_srids = [
            result[f'{k}Key'] for k in GBIF_RANKS if k in result.keys()] + ([result['key']] if 'key' in result else [])
        classification_srids = list(set(classification_srids))
        
        try:
            rank_order = GBIF_RANKS.index(result['rank'].lower())
        except ValueError:
            rank_order = None

        out_kwargs = {
            "source_id": GBIF_SOURCE_KEY,
            "source_name": GBIF_SOURCE_NAME,
            "source_record_id": result["key"],
            "scientific_name": result["canonicalName"],
            "authorship": strip_authorship(result['authorship']),
            "rank": result['rank'].lower(),
            "rank_order": rank_order,
            "classification_srids": classification_srids,
            "valid": True,
            "valid_srid": result["key"],
            "match_type": match_species["matchType"].lower() if is_valid else None,
            "is_parent": is_parent
        }
        out.append(cls(**out_kwargs))


        # Create rows for parent taxons
        for rank_order, rank in enumerate(GBIF_RANKS):
            if rank == result['rank'].lower():
                break
            if rank not in result.keys():
                continue
            taxa = result[rank]
            srid = result[rank + 'Key']
            match_type = None
            if rank == result["rank"].lower():
                valid_authorship = strip_authorship(result['authorship'])
                is_parent = False
                if is_valid:
                    match_type = match_species["matchType"].lower()
            else:
                valid_authorship = None
                is_parent = True
            out_kwargs = {
                    "source_id": GBIF_SOURCE_KEY,
                    "source_record_id": srid,
                    "source_name": GBIF_SOURCE_NAME,
                    "scientific_name": taxa,
                    "authorship": valid_authorship,
                    "rank": rank,
                    "rank_order": rank_order,
                    "classification_srids":classification_srids[:rank_order + 1],
                    "valid": True,
                    "valid_srid": srid,
                    "match_type": match_type,
                    "is_parent": is_parent
                }
            out.append(cls(**out_kwargs))                
        
        return out
    
    @classmethod
    def _prune_parent_taxa(cls, taxa_ref_list: List[TaxaRef], parent_taxa: str):
        out = []
        # List of ids of taxa_ref rows to keep corresponding to the parent taxa
        parent_srids = []
        keep_ids = set()

        # Find branches with parent_srids
        for ref in taxa_ref_list:
            if ref.scientific_name == parent_taxa:
                # Extend set of parent_srids
                parent_srids.append(ref.source_record_id)

                # Extend grand_parent_srids
                keep_ids.update(ref.classification_srids[:-1])


        # Find whole branches with parents srids
        for ref in taxa_ref_list:
            # Keep all nodes in branches with parent_srids and add to keep_names
            if any([srid in ref.classification_srids for srid in parent_srids if ref.classification_srids]):
                keep_ids.update(ref.classification_srids)

        # Special cases for sources without whole branch starting from kingdom
        for ref in taxa_ref_list:
            for source in SOURCES_PARENT_CLASSIFICATION_SRIDS:
                # Check if parent is listed for sources without whole branch
                if ref.source_name == source['source_name'] and \
                        parent_taxa in source['scienfitic_name']:
                    # Add ref to keep_ids
                    keep_ids.update([ref.source_record_id])

        # Keep only the rows with ids in keep_ids
        out = [ref for ref in taxa_ref_list if ref.valid_srid in keep_ids]
        
        return out
    
    @classmethod
    def from_custom_sources_fuzzy_matched(cls, fuzzy_name: str, match_type: str = None):
        out_custom = []
        
        out_bryoquel = cls.from_bryoquel(fuzzy_name)
        for match in out_bryoquel if out_bryoquel else []: # Loops over out_bryoquel if not empy
            if not match.is_parent: # for each item check if parent is true
                match.match_type = match_type # if it IS parent, then set match_type accordingly
            out_custom.append(match)
        
        out_cdpnq = cls.from_cdpnq(fuzzy_name)
        for match in out_cdpnq if out_cdpnq else []:
            if not match.is_parent:
                match.match_type = match_type
            out_custom.append(match)
                
        return out_custom

    @classmethod
    def from_all_sources(cls, name: str, authorship: str = None, parent_taxa: str = None):
        # Capitalize first letter
        name = name.strip()
        name = name[0].upper() + name[1:]
            
        out = cls.from_global_names(name, authorship)
        out.extend(cls.from_gbif(name, authorship))
        out.extend(cls.from_bryoquel(name)) # exact match only
        out.extend(cls.from_cdpnq(name)) # exact match only
        
        # Fuzzy match for custom sources
        if not any(ref.source_name in ('CDPNQ', 'Bryoquel') for ref in out):
            fuzzy_names = set({(ref.scientific_name, ref.match_type) for ref in out
                               if not ref.is_parent and ref.match_type != 'exact'})
        else : 
            fuzzy_names = set()

        # If input name is complex, then add exact match for fuzzy names
        if is_complex(name):
            fuzzy_names.update({(ref.scientific_name, ref.match_type) for ref in out
                    if not ref.is_parent})
        
        if fuzzy_names:
            for fuzzy_name, match_type in fuzzy_names:
                out.extend(cls.from_custom_sources_fuzzy_matched(fuzzy_name, match_type))
            
        if is_complex(name):
            out = cls.set_complex_match_type(out)

        # Prune for parent taxa
        if parent_taxa:
            out = cls._prune_parent_taxa(out, parent_taxa)
        
        # Remove duplicated records
        # (same taxonomic branch but for synonym of targeted taxons)
        out_dict = {}
        for ref in out:
            # Populate out_dict
            if ref.source_record_id not in out_dict:
                out_dict[ref.source_record_id] = ref
            # Replace entry in out_dict for a same srid if its match_type is `exact`
            elif ref.match_type == 'exact':
                out_dict[ref.source_record_id] = ref
        
        # Extract values from the dictionary
        out = list(out_dict.values()) 

        return out

    @classmethod
    def set_complex_match_type(cls, taxa_ref_list: List[TaxaRef]):
        out = []
        
        # Eliminate duplicates
        taxa_ref_list = {
            str(ref.__dict__): ref for ref in taxa_ref_list
            }.values()

        source_names = {ref.source_name for ref in taxa_ref_list}
        source_refs = {source_name: {} for source_name in source_names}
        source_rank_count = {source_name: {} for source_name in source_names}

        # Store unique refs in dict by source and count rank duplicates (complex)
        for ref in taxa_ref_list:
            try:
                source_refs[ref.source_name][ref.scientific_name]= ref
            except KeyError:
                source_refs[ref.source_name] = {ref.scientific_name: ref}
                source_rank_count[ref.source_name] = {}

            try:
                source_rank_count[ref.source_name][str(ref.rank_order)] += 1
            except KeyError:
                source_rank_count[ref.source_name][str(ref.rank_order)] = 1

        for source in source_refs.keys():
            # order refs by rank order
            source_set = sorted(
                source_refs[source].values(), key = lambda x: x.rank_order
                )
            # Set match_type by their position in rank_order
            complex_switch = False
            for i, ref in enumerate(source_set):
                if not complex_switch:
                    # Can enter here and switch only once
                    complex_switch = ref.rank_order == source_set[i - 1].rank_order
                    if complex_switch:
                        source_set[i - 2].match_type = "complex_closest_parent"
                        source_set[i - 1].match_type = "complex"

                if complex_switch:
                    ref.match_type = "complex"
            out.extend(source_set)
        return out

    @classmethod
    def from_bryoquel(cls, name: str):
        match_taxa = bryoquel.match_taxa(name)
        if match_taxa is None:
            return []
        
        out = [
            cls(
                source_id=BRYOQUEL_SOURCE_KEY,
                source_name=BROQUEL_SOURCE_NAME,
                source_record_id=match_taxa["id"],
                scientific_name=match_taxa["scientific_name"],
                authorship=match_taxa["authorship"],
                rank=match_taxa["taxon_rank"],
                rank_order=GBIF_RANKS.index('species'),
                valid=True,
                valid_srid=match_taxa["id"],
                match_type="exact",
                is_parent=False
            )]


        # Create rows for genus
        if out[0].rank == 'species':
            out.append(cls(
                source_id=BRYOQUEL_SOURCE_KEY,
                source_name=BROQUEL_SOURCE_NAME,
                source_record_id=match_taxa["genus"].lower(),
                scientific_name=match_taxa["genus"],
                authorship=None,
                rank='genus',
                rank_order=GBIF_RANKS.index('genus'),
                valid=True,
                valid_srid=match_taxa["genus"].lower(),
                match_type=None,
                is_parent=True
            ))

        # Create rows for family
        if out[0].rank in ['species', 'genus']:
            out.append(cls(
                source_id=BRYOQUEL_SOURCE_KEY,
                source_name=BROQUEL_SOURCE_NAME,
                source_record_id=match_taxa["family"].lower(),
                scientific_name=match_taxa["family"],
                authorship=None,
                rank='family',
                rank_order=GBIF_RANKS.index('family'),
                valid=True,
                valid_srid=match_taxa["family"].lower(),
                match_type=None,
                is_parent=True
            ))
        return out

    @classmethod
    def from_cdpnq(cls, name: str):
        out = []

        refs = cdpnq.match_taxa(name)
        if refs is None:
            return []
        for ref in refs:
            if ref["synonym"]:
                valid_match = cdpnq.match_taxa(ref["valid_name"])
                if valid_match:
                    valid_match = valid_match[0]
                    out.append(
                        cls(
                            source_id=CDPNQ_SOURCE_KEY,
                            source_name=CDPNQ_SOURCE_NAME,
                            source_record_id=valid_match["name"],
                            scientific_name=valid_match["name"],
                            authorship=valid_match["author"],
                            rank=valid_match["rank"],
                            rank_order=GBIF_RANKS.index(valid_match["rank"]),
                            valid=True,
                            valid_srid=valid_match["name"],
                            match_type=None,
                            is_parent=False
                        )
                )
                ref['valid'] = False
                ref['valid_srid'] = valid_match["name"]
            else:
                ref['valid'] = True
                ref['valid_srid'] = ref["name"]
                valid_match = ref
            
            out.append(cls(
                    source_id=CDPNQ_SOURCE_KEY,
                    source_name=CDPNQ_SOURCE_NAME,
                    source_record_id=ref["name"],
                    scientific_name=ref["name"],
                    authorship=ref["author"],
                    rank=ref["rank"],
                    rank_order=GBIF_RANKS.index(ref["rank"]),
                    valid=ref['valid'],
                    valid_srid=ref['valid_srid'],
                    match_type="exact",
                    is_parent=False
                )
            )

            # Create rows for valid parent genus
            if valid_match and valid_match['rank'].lower() in ['subspecies', 'species']:
                genus = valid_match['genus'] if 'genus' in valid_match else valid_match['name'].split(' ')[0] # Fallback when genus is not provided (current case for odonates)
                out.append(cls(
                    source_id=CDPNQ_SOURCE_KEY,
                    source_name=CDPNQ_SOURCE_NAME,
                    source_record_id = genus,
                    scientific_name=genus,
                    authorship=None,
                    rank='genus',
                    rank_order=GBIF_RANKS.index('genus'),
                    valid=True,
                    valid_srid=genus,
                    match_type=None,
                    is_parent=True
                ))

            # Create rows for valid parent species
            if valid_match and valid_match['rank'].lower() in ['subspecies']:
                species = valid_match['species'] if 'species' in valid_match else ' '.join(valid_match['name'].split(' ')[:2]) # Fallback when species is not provided (current case for odonates)
                out.append(cls(
                    source_id=CDPNQ_SOURCE_KEY,
                    source_name=CDPNQ_SOURCE_NAME,
                    source_record_id=species.lower(),
                    scientific_name=species,
                    authorship=None,
                    rank='species',
                    rank_order=GBIF_RANKS.index('species'),
                    valid=True,
                    valid_srid=species.lower(),
                    match_type=None,
                    is_parent=True
                ))

        # Remove duplicates
        out = {ref.source_record_id: ref for ref in out}.values()
        out = list(out)

        return out

def is_complex(name):
    return "|" in name


def find_authorship(name, name_simple):
    authorship = name.replace(name_simple, '')
    authorship = strip_authorship(authorship)

    return authorship

def strip_authorship(authorship):
    authorship = authorship.strip()
    try:
        if authorship[0] == '(' and authorship[-1] == ')' \
                and authorship.count('(') <= 1:
            authorship = authorship.lstrip('(').rstrip(')')
    except IndexError:
        pass
    return authorship
