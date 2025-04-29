# Test for the Eliso match function using unittest

import unittest
from bdqc_taxa import eliso

class TestEliso(unittest.TestCase):
    def test_match_species(self, name = 'Dysdera crocata'):
        print(name)
        result = eliso.match_taxa(name)
        self.assertEqual(result['taxa_name'], name)
        self.assertEqual(result['taxa_rank'], 'species')

    def test_no_match(self, name = 'Victor Cameron'):
            result = eliso.match_taxa(name)
            self.assertEqual(result, None)

    def test_match_genus(self, name = 'Argiope'):
        result = eliso.match_taxa(name)
        self.assertEqual(result['vernacular_fr'], 'Argiopes')
        self.assertEqual(result['taxa_rank'], 'genus')