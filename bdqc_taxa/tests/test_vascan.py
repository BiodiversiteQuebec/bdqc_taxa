# Test for the vascan remap functions using unittest

import unittest

from bdqc_taxa import vascan

# VASCAN taxon IDs used in the tests, named after the taxon they refer to
ARNICA_GRISCOMII = '2860'                          # species
ARNICA_GRISCOMII_GRISCOMII = '2861'                # subspecies, only infra-taxon in Quebec
TORREYOCHLOA_PALLIDA = '7981'                      # species, two infra-taxa in Quebec
CAKILE_EDENTULA = '9885'                           # species
CAKILE_EDENTULA_EDENTULA = '3936'                  # subspecies
CAKILE_EDENTULA_EDENTULA_EDENTULA = '3935'         # variety, only infra-taxon in Quebec


class TestVascanMatchTaxa(unittest.TestCase):
    def test_species_remapped_to_its_only_subspecies(self):
        """Arnica griscomii -> Arnica griscomii griscomii"""
        result = vascan.match_taxa(ARNICA_GRISCOMII)
        self.assertEqual(result['name'], 'Arnica griscomii')
        self.assertEqual(result['valid_taxon_id'], ARNICA_GRISCOMII_GRISCOMII)
        self.assertEqual(result['valid_name'], 'Arnica griscomii griscomii')
        self.assertEqual(result['valid_rank'], 'subspecies')

    def test_species_with_two_infra_taxa_in_quebec_not_remapped(self):
        """Torreyochloa pallida has two infra-taxa in Quebec, so it is kept as is"""
        self.assertIsNone(vascan.match_taxa(TORREYOCHLOA_PALLIDA))

    def test_infra_taxon_target_not_remapped(self):
        """Arnica griscomii griscomii is a remap target, not a remapped taxon"""
        self.assertIsNone(vascan.match_taxa(ARNICA_GRISCOMII_GRISCOMII))

    def test_subspecies_remapped_to_its_only_variety(self):
        """Cakile edentula edentula -> Cakile edentula edentula edentula"""
        result = vascan.match_taxa(CAKILE_EDENTULA_EDENTULA)
        self.assertEqual(result['name'], 'Cakile edentula edentula')
        self.assertEqual(result['valid_taxon_id'], CAKILE_EDENTULA_EDENTULA_EDENTULA)
        self.assertEqual(result['valid_name'], 'Cakile edentula edentula edentula')
        self.assertEqual(result['valid_rank'], 'variety')


class TestVascanIntermediateTaxa(unittest.TestCase):
    def test_no_intermediate_when_target_is_direct_child(self):
        """Arnica griscomii -> Arnica griscomii griscomii: nothing in between"""
        remap = vascan.match_taxa(ARNICA_GRISCOMII)
        self.assertEqual(vascan.get_intermediate_taxa(remap), [])

    def test_subspecies_between_species_and_variety(self):
        """Cakile edentula -> (Cakile edentula edentula) -> Cakile edentula edentula edentula"""
        remap = vascan.match_taxa(CAKILE_EDENTULA)
        self.assertEqual(remap['valid_taxon_id'], CAKILE_EDENTULA_EDENTULA_EDENTULA)

        intermediates = vascan.get_intermediate_taxa(remap)
        self.assertEqual([row['taxon_id'] for row in intermediates], [CAKILE_EDENTULA_EDENTULA])
        self.assertEqual(intermediates[0]['name'], 'Cakile edentula edentula')
        self.assertEqual(intermediates[0]['rank'], 'subspecies')

    def test_no_intermediate_from_subspecies(self):
        """Cakile edentula edentula -> Cakile edentula edentula edentula: nothing in between"""
        remap = vascan.match_taxa(CAKILE_EDENTULA_EDENTULA)
        self.assertEqual(vascan.get_intermediate_taxa(remap), [])


if __name__ == '__main__':
    unittest.main()
