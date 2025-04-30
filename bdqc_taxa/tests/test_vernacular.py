from unittest import TestCase, result
from bdqc_taxa.vernacular import Vernacular, initcap_vernacular

class TestVernacular(TestCase):
    def assertVernacularList(self, results):
        self.assertTrue(results.__len__() >= 1)
        self.assertTrue(all([vn.language in ['fra', 'eng'] for vn in results]))
        for result in results:
            self.assertNotEqual(result.rank_order, 9999)

    def test_from_gbif(self, gbif_key = 2474953, rank = 'species'):
        results = Vernacular.from_gbif(gbif_key, rank)
        self.assertVernacularList(results)

    def test_from_gbif_no_rank(self, gbif_key = 2474953):
        results = Vernacular.from_gbif(gbif_key)
        for result in results:
            self.assertEqual(result.rank_order, 9999)
    
    def test_from_gbif_non_unique(self, gbif_key = 2474953):
        results = Vernacular.from_gbif(gbif_key)
        unique_results = {str(vars(o)): o for o in results}.values()
        self.assertEqual(len(results), len(unique_results))


    def test_from_gbif_invalid_key(self, gbif_key = 9036008, rank = 'species'):
        results = Vernacular.from_gbif(gbif_key, rank)
        self.assertVernacularList(results)

    def test_from_gbif_match(self, name = 'Cyanocitta cristata'):
        results = Vernacular.from_gbif_match(name)
        self.assertVernacularList(results)
    
    def test_from_gbif_match_no_match(self, name = 'Vincent Beauregard'):
        results = Vernacular.from_gbif_match(name)
        self.assertIsInstance(results, list)
        
        # Assert empty list
        self.assertEqual(len(results), 0)

    def test_from_gbif_match_passing_kwargs(self,
        name = 'Cyanocitta cristata',
        kingdom = 'Animalia'):
        results = Vernacular.from_gbif_match(name, kingdom = kingdom)
        self.assertVernacularList(results)

    def test_from_gbif_preferred_epilobium_ciliatum(
            self, name = 'Epilobium ciliatum var. ecomosum',
            source = 'Database of Vascular Plants of Canada (VASCAN)'):
        results = Vernacular.from_gbif_match(name)
        self.assertVernacularList(results)

        # Filter for source
        results = [vn for vn in results if vn.source == source]

        # Assert some results are preferred
        self.assertTrue(any([vn.preferred for vn in results]))

        # Assert some results are not preferred
        self.assertTrue(any([not vn.preferred for vn in results]))

    def test_from_match(self, name = 'Cyanocitta cristata'):
        results = Vernacular.from_match(name)
        self.assertVernacularList(results)
    
    def test_from_match_no_match(self, name = 'Vincent Beauregard'):
        results = Vernacular.from_match(name)
        self.assertIsInstance(results, list)
        
        # Assert empty list
        self.assertEqual(len(results), 0)

    def test_from_bryoquel_match(self, name = 'Aulacomnium palustre'):
        results = Vernacular.from_bryoquel_match(name)
        self.assertVernacularList(results)
        for result in results:
            self.assertNotEqual(result.rank_order, 9999)

    def test_from_bryoquel_no_vernacular(self, name = 'Aulacomnium'):
        results = Vernacular.from_bryoquel_match(name)
        self.assertFalse(results)

    def test_from_bryoquel_no_match(self, name = 'Vincent Beauregard'):
        results = Vernacular.from_bryoquel_match(name)
        self.assertIsInstance(results, list)
        
        # Assert empty list
        self.assertEqual(len(results), 0)
    
    def test_from_cdpnq_match(self, name = 'Libellula luctuosa'):
        results = Vernacular.from_cdpnq_match(name)
        self.assertVernacularList(results)
        self.assertTrue(any([vn.source == 'CDPNQ' for vn in results]))

    def test_from_cdpnq_no_match(self, name = 'Vincent Beauregard'):
        results = Vernacular.from_cdpnq_match(name)
        self.assertFalse(results)

    def test_match_english_cdpnq(self, name = 'Perimyotis subflavus'):
        result = Vernacular.from_cdpnq_match(name)
        self.assertTrue(any(item.language == 'eng' for item in result))

    # Special case test: Synonym scientific name is not in CDPNQ : Bug #5
    # Removed. We no longer expect Vernacular to return vernacular names for
    # synonyms.
    # def test_from_match_synonym_cdpnq(self, name = 'Libellula julia'):
    #     results = Vernacular.from_match(name)
    #     self.assertVernacularList(results)
    #     self.assertTrue(any([vn.source == 'CDPNQ' for vn in results]))

    def test_from_match_bad_cap(self, name = 'Passer domesticus'):
        results = Vernacular.from_match(name)
        self.assertVernacularList(results)
        self.assertTrue(any([vn.source == 'CDPNQ' for vn in results]))
        # Asser that the vernacular is 'Moineau domestique'
        self.assertTrue(any([vn.name == 'Moineau domestique' for vn in results]))

    def test_wikidata_chiroptera(self, name = 'Chiroptera', rank = 'order'):	
        results = Vernacular.from_wikidata_match(name, rank = rank)
        self.assertVernacularList(results)
        self.assertTrue(any([vn.source == 'Wikidata' for vn in results]))
        self.assertTrue(any([vn.language == 'eng' for vn in results]))
        self.assertTrue(any([vn.language == 'fra' for vn in results]))

    def test_wikidata_no_rank(self, name = 'Chiroptera'):
        results = Vernacular.from_wikidata_match(name)
        for result in results:
            self.assertEqual(result.rank_order, 9999)

    def test_wikidata_no_fr_aliases(self, name = 'Lasionycteris'):
        # Assert no error is raised
        results = Vernacular.from_wikidata_match(name)

    def test_wikidata_no_match(self, name = 'Vincent Beauregard'):
        results = Vernacular.from_wikidata_match(name)
        self.assertIsInstance(results, list)
        
        # Assert empty list
        self.assertEqual(len(results), 0)

    def test_wikidata_return_only_taxons(self, query='Alcea', bad_name = 'Alcea (given name)'):
        # Previous error where scientific name Alcea match a personality in wikidata but not a taxon
        results = Vernacular.from_wikidata_match(query)
        # Assert no `(given name)`` string in results
        self.assertFalse(any([bad_name in vn.name for vn in results]))


    def test_wikidata_filters_scientific_name(self, query='Acer saccharum saccharum', bad_name = 'Acer saccharum'):
        # Previous error where scientific name is in french or english and is not desired.
        results = Vernacular.from_wikidata_match(query)
        # Assert no scientific name in results
        self.assertFalse(any([bad_name in vn.name for vn in results]))
        
    def test_gbif_with_rank_epilobium(self, name='Epilobium ciliatum ciliatum', rank='subspecies'):
        result = Vernacular.from_gbif_match(name, rank)
        self.assertTrue(all([vn.rank == 'subspecies' for vn in result]))
        
    def test_wikidata_bad_rank_no_error(self, name='Embryophyta', rank='superdivision'):
        # Rank not in the list of accepted ranks in wikidata and it previously failed
        Vernacular.from_match(name=name, rank=rank)

class TestInitcap(TestCase):
    def test_initcap_vernacular(self, text = 'Vincent Beauregard'):
        self.assertEqual(initcap_vernacular(text), 'Vincent beauregard')
    
    def test_initcap_no_change(self, text = 'Lynx du Canada'):
        self.assertEqual(initcap_vernacular(text), text)

    def test_initcap_all_caps(self, text = 'LYNX DU CANADA'):
        self.assertEqual(initcap_vernacular(text), 'Lynx du Canada')
    
    def test_initcap_all_lower(self, text = 'millepertuis de fraser'):
        self.assertEqual(initcap_vernacular(text), 'Millepertuis de Fraser')

    def test_initcap_nordique(self, text = 'Chauve-souris nordique'):
        self.assertEqual(initcap_vernacular(text), text)

    def test_initcap_moineau(self, text = 'Moineau domestique'):
        self.assertEqual(initcap_vernacular(text), text)