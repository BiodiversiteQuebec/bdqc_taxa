# Test for the cdpnq match function using unittest

import unittest

from bdqc_taxa import cdpnq

class TestCdpnqOdonates(unittest.TestCase):
    def test_match_species(self, name = 'Libellula luctuosa'):
        result = cdpnq.match_taxa_odonates(name)
        self.assertEqual(result['name'], name)
        self.assertEqual(result['rank'], 'species')
        
    def test_no_match(self, name = 'Vincent Beauregard'):
        result = cdpnq.match_taxa_odonates(name)
        self.assertEqual(result, None)

    def test_match_synonym(self, name = 'Gomphus borealis'):
        result = cdpnq.match_taxa_odonates(name)
        self.assertEqual(result['valid_name'], 'Phanogomphus borealis')
        self.assertEqual(result['rank'], 'species')

    def test_match_genus(self, name = 'Libellula'):
        result = cdpnq.match_taxa_odonates(name)
        self.assertEqual(result['name'], 'Libellula')
        self.assertEqual(result['rank'], 'genus')

    def test_ladona_julia(self, name = 'Ladona julia'):
        result = cdpnq.match_taxa_odonates(name)
        self.assertTrue(result['name'] == 'Ladona julia')
        self.assertTrue(result['valid_name'] == 'Ladona julia')
        self.assertTrue(result['rank'] == 'species')
        
class TestCdpnqVertebrates(unittest.TestCase):
    def test_match_species(self, name = 'Pica hudsonia'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['name'], name)
        self.assertEqual(result['rank'], 'species')
    
    def test_match_species_has_source_datasets_id(self, name = 'Pica hudsonia', datasets_id = '9b779078-1fd1-4492-8bbe-0892b0d13192'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['source_dataset_id'], datasets_id)

    def test_no_match(self, name = 'Vincent Beauregard'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result, None)

    def test_match_synonym(self, name = 'Pica pica'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['valid_name'], 'Pica hudsonia')
        self.assertEqual(result['rank'], 'species')

    def test_match_synonym_from_gbif(self, name = 'Leuconotopicus villosus'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['valid_name'], 'Dryobates villosus')
        self.assertEqual(result['rank'], 'species')
        self.assertEqual(result['synonym'], 1)


    def test_match_genus(self, name = 'Pica'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['name'], 'Pica')
        self.assertEqual(result['rank'], 'genus')

    def test_match_genus_synonym(self, name = 'Rana'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['name'], 'Rana')
        self.assertEqual(result['rank'], 'genus')
        self.assertEqual(result['synonym'], 1)
        self.assertEqual(result['valid_name'], 'Lithobates')
        # All species in Quebec are now in Lithobates genus

    def test_match_ambiguous_genus_resolution(self, name = 'Parus'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertIsNone(result)
        # Genus Parus is related to either species from genus Poecile or Baeolophus
        # and thus cannot be resolved to a single genus.
    
    def test_rangifer_tarandus(self, name = 'Rangifer tarandus'):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertTrue(result['valid_name'] == 'Rangifer tarandus caribou')
        self.assertTrue(result['rank'] == 'species')
        self.assertTrue(result['synonym'] == 1)
        self.assertTrue(result['vernacular_fr'] == 'Caribou des bois')
        self.assertTrue(result['vernacular_en'] == 'Woodland Caribou')

# Species with only one infra-taxon in Quebec are remapped to that infra-taxon
class TestCdpnqVertebratesRemap(unittest.TestCase):
    def assert_remap(self, name, valid_name):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['valid_name'], valid_name)
        self.assertEqual(result['synonym'], 1)

    def assert_not_remapped(self, name):
        result = cdpnq.match_taxa_vertebrates(name)
        self.assertEqual(result['valid_name'], name)
        self.assertEqual(result['synonym'], 0)

    def test_remap_to_subspecies(self):
        self.assert_remap('Nerodia sipedon', 'Nerodia sipedon sipedon')

    def test_remap_to_population(self):
        self.assert_remap('Desmognathus ochrophaeus', 'Desmognathus ochrophaeus pop. 2')

    def test_remap_to_subspecies_with_populations(self):
        # Rangifer tarandus caribou has 5 populations, which can't be told apart
        self.assert_remap('Rangifer tarandus', 'Rangifer tarandus caribou')
        self.assert_not_remapped('Rangifer tarandus caribou')

    def test_remap_falco_peregrinus(self):
        # Falco peregrinus pop. 1 is the anatum/tundrius population
        for name in ['Falco peregrinus', 'Falco peregrinus anatum', 'Falco peregrinus tundrius']:
            self.assert_remap(name, 'Falco peregrinus pop. 1')
        self.assert_not_remapped('Falco peregrinus pop. 1')

    def test_remap_synonym_of_remapped_species(self):
        self.assert_remap('Oceanodroma leucorhoa', 'Hydrobates leucorhous pop. 1')
        self.assert_remap('Chrysemys scripta', 'Trachemys scripta elegans')

    def test_remap_species_listed_infra_with_status(self):
        # Listed themselves, but their only infra-taxon has a CDPNQ status
        self.assert_remap('Osmerus mordax', 'Osmerus mordax pop. 1')
        self.assert_remap('Coregonus artedi', 'Coregonus artedi pop. 1')
        self.assert_remap('Salvelinus alpinus', 'Salvelinus alpinus oquassa')
        self.assert_remap('Phoca vitulina', 'Phoca vitulina mellonae')

    def test_remap_synonym_of_remapped_listed_species(self):
        self.assert_remap('Salvelinus salvelinus', 'Salvelinus alpinus oquassa')

    def test_no_remap_species_listed_infra_without_status(self):
        # Loxia curvirostra is listed itself (S5), Loxia curvirostra percna (S2) has no status
        self.assert_not_remapped('Loxia curvirostra')
        self.assert_not_remapped('Salmo salar')

    def test_no_remap_many_populations(self):
        self.assert_not_remapped('Delphinapterus leucas')

    def test_no_remap_many_subspecies(self):
        self.assert_not_remapped('Eremophila alpestris')

# Test match_taxa for both odonates and vertebrates
class TestCdpnq(unittest.TestCase):
    def test_match_species(self, name = 'Libellula luctuosa'):
        result = cdpnq.match_taxa(name)
        self.assertEqual(result[0]['name'], name)
        self.assertEqual(result[0]['rank'], 'species')

    def test_no_match(self, name = 'Vincent Beauregard'):
        result = cdpnq.match_taxa(name)
        self.assertFalse(result)

    def test_match_vertebrates_species(self, name = 'Pica hudsonia'):
        result = cdpnq.match_taxa(name)
        self.assertEqual(result[0]['name'], name)
        self.assertEqual(result[0]['rank'], 'species')
