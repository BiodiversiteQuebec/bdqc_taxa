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
        result = search_species(name, operator='similarTo', match_against='allScientificNames')
        # Should return a dictionary with 'data' key as a list
        self.assertIsInstance(result, list)
        # No matches expected for misspelled name
        self.assertEqual(len(result), 0)

    def test_search_match_similar_to(self, name='Matteuccia struthiopteris'):
        # Notes on behavior:
        # 1. The search is case-insensitive.
        # 2. The search is not exact, so it may return more than one match.
        # 3. The search may return synonyms, so the result may not contain
        #    the exact scientific name.
        # 4. The search returns also infra-specific names

        # Test a correct species name returns at least one match
        result = search_species(name, operator='similarTo', match_against='allScientificNames')
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)
        # First result should contain 'scientificName'
        self.assertIn('scientificName', result[0])
        # record type of all results should be 'species'
        self.assertTrue(all(record.get('recordType') == 'SPECIES' for record in result))

    def test_search_match_exact(self, name='Matteuccia struthiopteris'):
        # Notes on behavior:
        # 1. Returns only the accepted name records but not infra-specific names
        
        # Test a correct species name returns at least one match
        result = search_species(name, operator='equals', match_against='allScientificNames')
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        # First result should contain 'scientificName'
        self.assertIn('scientificName', result[0])
        # record type of all results should be 'species'
        self.assertTrue(all(record.get('recordType') == 'SPECIES' for record in result))

        # Assert record of scientificName Matteuccia struthiopteris in the result
        self.assertTrue(any(record.get('scientificName') == name for record in result))

    def test_search_match_exact_case_insensitive(self, name='MATTEUCCIA STRUTHIOPTERIS'):
        # Notes on behavior:
        # 1. The search is case-sensitive so returns no match.

        # Test a correct species name returns at least one match
        result = search_species(name, operator='equals', match_against='allScientificNames')
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)


    def test_search_match_similar_to_fuzzy(self, name='Matteucia strutiopterys'):
        # Notes on behaviour on fuzzy search: works but not return any attributes indicating fuzzy match

        # Test a partial species name returns at least one match
        result = search_species(name, operator='similarTo', match_against='allScientificNames')
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)
        # First result should contain 'scientificName'
        self.assertIn('scientificName', result[0])
        # record type of all results should be 'species'
        self.assertTrue(all(record.get('recordType') == 'SPECIES' for record in result))

        # Assert record of scientificName Matteuccia struthiopteris in the result
        self.assertTrue(any(record.get('scientificName') == 'Matteuccia struthiopteris' for record in result))

    def test_search_match_synonym(self, name='Picoides villosus', accepted_name='Dryobates villosus'):
        # Notes on behaviour on synonym search: Returns only the accepted name records and infra-specific names

        # Test a synonym species name returns at least one match
        result = search_species(name, operator='similarTo', match_against='allScientificNames')
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)
        # First result should contain 'scientificName'
        self.assertIn('scientificName', result[0])
        # record type of all results should be 'species'
        self.assertTrue(all(record.get('recordType') == 'SPECIES' for record in result))

        # Assert record of scientificName Dryobates villosus in the result
        self.assertTrue(any(record.get('scientificName') == accepted_name for record in result))