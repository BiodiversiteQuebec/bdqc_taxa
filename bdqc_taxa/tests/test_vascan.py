# Test for the vascan remap functions using unittest

import unittest

from bdqc_taxa import vascan

class TestVascanRemap(unittest.TestCase):
    def test_match_species(self, taxon_id = '2860'):
        result = vascan.match_taxa(taxon_id)
        self.assertEqual(result['name'], 'Arnica griscomii')
        self.assertEqual(result['valid_taxon_id'], '2861')
        self.assertEqual(result['valid_name'], 'Arnica griscomii griscomii')
        self.assertEqual(result['valid_rank'], 'subspecies')

    def test_no_match_two_infra_taxa_in_quebec(self, taxon_id = '7981'):
        self.assertIsNone(vascan.match_taxa(taxon_id))

    def test_no_match_target_is_not_a_key(self, taxon_id = '2861'):
        self.assertIsNone(vascan.match_taxa(taxon_id))

    def test_intermediate_taxa_none(self, taxon_id = '2860'):
        remap = vascan.match_taxa(taxon_id)
        self.assertEqual(vascan.get_intermediate_taxa(remap), [])

    def test_intermediate_taxa_nested_variety(self, taxon_id = '9885'):
        remap = vascan.match_taxa(taxon_id)
        intermediates = vascan.get_intermediate_taxa(remap)
        self.assertEqual([row['taxon_id'] for row in intermediates], ['3936'])
        self.assertEqual(intermediates[0]['rank'], 'subspecies')

    def test_intermediate_taxa_from_subspecies(self, taxon_id = '3936'):
        remap = vascan.match_taxa(taxon_id)
        self.assertEqual(remap['valid_taxon_id'], '3935')
        self.assertEqual(vascan.get_intermediate_taxa(remap), [])

if __name__ == '__main__':
    unittest.main()
