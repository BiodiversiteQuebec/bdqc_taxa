from unittest import TestCase
from bdqc_taxa.natureserve import get_taxon, search_species


class TestNatureServe(TestCase):
    def test_get_taxon(self, global_id='ELEMENT_GLOBAL.2.160636'):
        result = get_taxon(global_id)
        # Should return a dictionary without an 'error' key
        self.assertIsInstance(result, dict)
        self.assertNotIn('error', result)
        # Should contain the requested global ID
        self.assertTrue(result.get('uniqueId') == global_id)

    def test_search_no_match(self, name='Dominique Gravel'):
        result = search_species(name)
        # Should return a dictionary with 'data' key as a list
        self.assertIsInstance(result, list)
        # No matches expected for misspelled name
        self.assertEqual(len(result), 0)

    def test_search_match(self, name='Matteuccia struthiopteris'):
        # Test a correct species name returns at least one match
        result = search_species(name)
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)
        # First result should contain 'scientificName'
        self.assertIn('scientificName', result[0])
        # record type of all results should be 'species'
        self.assertTrue(all(record.get('recordType') == 'SPECIES' for record in result))

    # Additional tests can be added here
