# Test the bryoquel module

from unittest import TestCase
from bdqc_taxa.bryoquel import match_species

class TestBryoquel(TestCase):
    def test_match_species(self, species='Aulacomnium palustre'):
        # Test a species that is in the database
        result = match_species(species)
        self.assertEqual(result['id'], 269)
        self.assertEqual(result['family_scientific_name'], 'Aulacomniaceae')
        self.assertEqual(result['species_scientific_name'], 'Aulacomnium palustre')
        self.assertEqual(result['species_canonical_name'], 'Aulacomnium palustre (Hedw.) Schwägr.')
        self.assertEqual(result['vernacular_name_fr'], 'aulacomnie des marais')
        self.assertEqual(result['vernacular_name_en'], 'ribbed bog moss')
        self.assertEqual(result['clade'], 'Musci')
        self.assertEqual(result['authorship'], '(Hedw.) Schwägr.')

    def test_match_species_canonical(self, species='Aulacomnium palustre (Hedw.) Schwägr.'):
        # Test a species that is in the database
        result = match_species(species)
        self.assertEqual(result['id'], 269)
        self.assertEqual(result['family_scientific_name'], 'Aulacomniaceae')
        self.assertEqual(result['species_scientific_name'], 'Aulacomnium palustre')
        self.assertEqual(result['vernacular_name_fr'], 'aulacomnie des marais')
        self.assertEqual(result['vernacular_name_en'], 'ribbed bog moss')
        self.assertEqual(result['clade'], 'Musci')
        self.assertEqual(result['authorship'], '(Hedw.) Schwägr.')