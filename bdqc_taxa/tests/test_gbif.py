from unittest import TestCase
from bdqc_taxa.gbif import Species
from typing import List


class TestSpecies(TestCase):
    def test_get(self, key = 9036008):
        result = Species.get(key=key)
        self.assertIsInstance(result, dict)
        self.assertTrue(all(k in result.keys() for k in [
            'key', 'scientificName'
        ]))
        self.assertTrue(all([v for k, v in result.items()
                             if k not in [
                                 'synonym', 'nomenclaturalStatus', 'remarks',
                                 'numDescendants', 'issues']]))

    def test_match_from_name(self, name='Antigone canadensis'):
        result = Species.match(scientific_name=name)
        self.assertIsInstance(result, dict)
        self.assertTrue(all(k in result.keys() for k in [
            'usageKey', 'scientificName'
        ]))
        self.assertTrue(all([v for k, v in result.items()
                             if k not in ['synonym']]))

    def test_match_from_name_kingdom(self,
                                     name='Coleoptera', kingdom='Plantae'):
        result = Species.match(scientific_name=name, kingdom = kingdom)
        self.assertIsInstance(result, dict)
        self.assertTrue(all(k in result.keys() for k in [
            'usageKey', 'scientificName'
        ]))
        self.assertTrue(all([v for k, v in result.items()
                             if k not in ['synonym']]))

    def test_get_vernacular_name(self, species_id=2474953):
        results = Species.get_vernacular_name(species_id)
        self.assertTrue(len(results) > 1)
        for result in results:
            self.assertTrue(all([v for k, v in result.items()
                                 if k not in ['preferred']]))
            
    def test_has_preferred(self, gbif_key=5231190):
        results = Species.get_vernacular_name(gbif_key)
        self.assertTrue(len(results) > 1)

        # At least one vernacular record has preferred key
        has_preferred = False
        for result in results:
            if 'preferred' in result and result['preferred']:
                has_preferred = True
        
        self.assertTrue(has_preferred)
    
    # Regression test : Should return subsp. record
    def test_species_match_no_rank(self, name='Epilobium ciliatum ciliatum'):
        result = Species.match(scientific_name=name)
        self.assertFalse(result['rank'] == 'SUBSPECIES')
    
    def test_species_match_rank(self, name='Epilobium ciliatum ciliatum', rank='SUBSPECIES'):
        result = Species.match(scientific_name=name, rank=rank)
        self.assertTrue(result['rank'] == 'SUBSPECIES')
    # Edge case with no rank returns most precise possible answer
    def test_species_match_bad_rank(self, name='Epilobium ciliatum ciliatum', rank='KINGDOM'):
        result = Species.match(scientific_name=name, rank=rank)
        self.assertTrue(result['rank'] == 'SPECIES')

    def test_species_match_bug_limax(self, name='Limax'):
        results = Species.match(scientific_name=name)
        self.assertIsInstance(results, List)
        self.assertTrue(len(results) > 0)
        self.assertTrue(all(k in results[0].keys() for k in [
            'usageKey', 'scientificName', 'rank'
        ]))
        self.assertTrue(results[0]['rank'] == 'GENUS')