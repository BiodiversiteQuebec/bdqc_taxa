import unittest

from bdqc_taxa import bryoquel, taxa_ref
from bdqc_taxa import global_names


class TestFindAuthorship(unittest.TestCase):
    def test_species_author(self):
        name = 'Grus canadensis (Linnaeus, 1758)'
        name_simple = 'Grus canadensis'
        authorship = taxa_ref.find_authorship(name, name_simple)
        self.assertEqual(authorship, "Linnaeus, 1758")

    # def test_species_author_du_roi(self):
    #     name = 'Grus canadensis (Linnaeus, 1758)'
    #     name_simple = 'Grus canadensis'
    #     authorship = taxa_ref.find_authorship(name, name_simple)
    #     self.assertEqual(authorship, "Linnaeus, 1758")

    def test_subspecies_author(self):
        name = 'Spinus tristis tristis (Linnaeus, 1758)'
        name_simple = 'Spinus tristis tristis'
        authorship = taxa_ref.find_authorship(name, name_simple)
        self.assertEqual(authorship, "Linnaeus, 1758")

    def test_genus_author(self):
        name = 'Acer L.'
        name_simple = 'Acer'
        authorship = taxa_ref.find_authorship(name, name_simple)
        self.assertEqual(authorship, "L.")

    def test_family_author(self):
        name = 'Passeridae Rafinesque, 1815'
        name_simple = 'Passeridae'
        authorship = taxa_ref.find_authorship(name, name_simple)
        self.assertEqual(authorship, "Rafinesque, 1815")


class TestTaxaRef(unittest.TestCase):
    def test_dict_dunder(self):
        name = 'Lasiurus cinereus'
        tr = taxa_ref.TaxaRef(name)
        self.assertTrue(isinstance(tr.__dict__, dict))

    def test_repr(self):
        name = 'Lasiurus cinereus'
        tr = taxa_ref.TaxaRef(name)
        self.assertEqual(repr(tr), f"TaxaRef('{name}')")
    
    def test_str(self):
        name = 'Lasiurus cinereus'
        tr = taxa_ref.TaxaRef(name)
        self.assertEqual(str(tr), name)

    def test_from_global_names(self, name='Acer saccharum'):
        refs = taxa_ref.TaxaRef.from_global_names(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order', 'is_parent']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(all([ref.rank != 11 for ref in refs]))

        # Bug: Bad authorship value, equals scientific_name
        bad_refs = [
            ref for ref in refs
            if ref.authorship
                and ref.authorship == ref.scientific_name
        ]
        self.assertFalse(bad_refs)


    def test_from_global_names_no_match(self, name='Vincent Beauregard'):
        refs = taxa_ref.TaxaRef.from_global_names(name)
        self.assertFalse(refs)

    def test_from_gbif(self, name='Acer saccharum'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order', 'is_parent']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(len({(ref.valid, ref.rank_order) for ref in refs}) ==
                        len(refs))

        # Bug: Bad authorship value, equals scientific_name
        bad_refs = [
            ref for ref in refs
            if ref.authorship
                and ref.authorship == ref.scientific_name
        ]
        self.assertFalse(bad_refs)
    
    def test_from_cdpnq(self, name='Lestes vigilax'):
        refs = taxa_ref.TaxaRef.from_cdpnq(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order', 'is_parent', 'classification_srids']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(len({(ref.valid, ref.rank_order) for ref in refs}) ==
                        len(refs))

        # Bug: Bad authorship value, equals scientific_name
        bad_refs = [
            ref for ref in refs
            if ref.authorship
                and ref.authorship == ref.scientific_name
        ]
        self.assertFalse(bad_refs)
        
    def test_from_cdpnq_no_match(self, name='Vincent Beauregard'):
        refs = taxa_ref.TaxaRef.from_cdpnq(name)
        self.assertFalse(refs)
    
    def test_from_all_sources_cdpnq(self, name='Libellula luctuosa'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) > 1)
        
        # Assert any ref is from CDPNQ
        self.assertTrue(any([ref.source_id == 1002 for ref in refs]))
        self.assertTrue(any([ref.source_name == 'CDPNQ' for ref in refs]))

    def test_from_all_sources_cdpnq_fuzzy_synonym(self, name='Gomphus exils'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) > 1)
        
        # Assert any ref is from CDPNQ
        self.assertTrue(any([ref.source_id == 1002 for ref in refs]))
        self.assertTrue(any([ref.source_name == 'CDPNQ' for ref in refs]))
  
    def test_from_all_sources_cdpnq_synonym(self, name = 'Libellula julia'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        cdpnq_refs = [
            ref for ref in refs
            if ref.source_name == 'CDPNQ' and ref.match_type != 'exact'
        ]
        self.assertGreater(len(cdpnq_refs), 0)

    # Non-regression test for bug for 'Clupea harengus harengus'
    def test_from_all_sources_bug(self, name = 'Clupea harengus harengus'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) > 1)

    def test_from_gbif(self, name='Antigone canadensis'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order',
                      'is_parent', 'valid']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(len({(ref.valid, ref.rank_order) for ref in refs}) ==
                        len(refs))

        # Bug: Bad authorship value, equals scientific_name
        bad_refs = [
            ref for ref in refs
            if ref.authorship
                and ref.authorship == ref.scientific_name
        ]
        self.assertFalse(bad_refs)


    def test_from_gbif_bug_acceptedKey_error(
            self, name='Kobresia simpliciuscula subholarctica (T.V.Egorova) Saarela'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order',
                      'is_parent', 'valid']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(len({(ref.valid, ref.rank_order) for ref in refs}) ==
                        len(refs))

    def test_from_gbif_bug_rank_key_error(
            self, name='Ranunculus arcticus (R. Br.) L. D. Benson'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order',
                      'is_parent', 'valid']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))
        self.assertTrue(len({(ref.valid, ref.rank_order) for ref in refs}) ==
                        len(refs))

    def test_from_gbif_w_authorship(self, name='Acer saccharum',
                                    authorship='Pax'):
        refs = taxa_ref.TaxaRef.from_gbif(name, authorship)
        self.assertTrue(len(refs) > 1)
        [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
         if k not in ['id', 'match_type', 'authorship', 'rank_order',
                      'is_parent', 'valid']
         ]
        self.assertTrue(any([ref.match_type for ref in refs]))
        self.assertTrue(any([ref.authorship for ref in refs]))
        self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))
        self.assertTrue(all([ref.rank.lower() == ref.rank for ref in refs]))

    def test_from_gbif_no_match(self, name='Vincent Beauregard'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertFalse(refs)

    def test_from_gbif_invalid_subspecies(self, name='Circus hudsonius'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        self.assertTrue(len(refs) > 1)
        self.assertTrue(any([ref.rank == 'subspecies' for ref in refs]))

    # Case where GBIF returns a higherrank match that should be classified as a parent
    def test_from_gbif_bug_higher_rank(self, name='Nereis'):
        refs = taxa_ref.TaxaRef.from_gbif(name)
        higherrank_refs = [
            ref for ref in refs
            if ref.match_type == 'higherrank'
        ]
        self.assertTrue(len(higherrank_refs) > 0)
        self.assertTrue(all([ref.is_parent == True for ref in higherrank_refs]))

    def test_from_all_sources(self, name='Acer saccharum'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) > 1)
        ref_sources_id = {ref.source_id for ref in refs}
        pref_sources = global_names.DATA_SOURCES + [11]
        self.assertTrue(all([v in ref_sources_id for v in pref_sources]))

    def test_from_all_sources_no_match(self, name='Vincent Beauregard'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertFalse(refs)

    def test_from_bryoquel(self, name='Anthelia julacea'):
        refs = taxa_ref.TaxaRef.from_bryoquel(name)
        self.assertTrue(len(refs) >= 1)
        self.assertTrue(all([ref.source_id == 1001 for ref in refs]))
        # Assert there is a family
        self.assertTrue(any([ref.rank == 'family' for ref in refs]))

        # Assert there is a genus
        self.assertTrue(any([ref.rank == 'genus' for ref in refs]))

        # Assert there is a genus
        self.assertTrue(any([ref.rank == 'genus' for ref in refs]))

    def test_from_bryoquel_no_match(self, name='Vincent Beauregard'):
        refs = taxa_ref.TaxaRef.from_bryoquel(name)
        self.assertFalse(refs)

    def test_from_all_sources_bryoquel(self, name='Anthelia julacea'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) >= 1)
        self.assertTrue(any([ref.source_id == 1001 for ref in refs]))

    def test_fuzzy_match_bryoquel(self, name='Anthelia julaca'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) >= 1)

        bryoquel_matches = [ref for ref in refs if ref.source_id == 1001]
        self.assertTrue(any(bryoquel_matches))
        self.assertTrue(any([ref.scientific_name == 'Anthelia julacea' for ref in bryoquel_matches]))

    def test_bubo_parent_parentless_output_len(self, name='Bubo scandiacus', parent_scientific_name='Chordata'):
        results_parent = taxa_ref.TaxaRef.from_all_sources(name)
        results_parentless = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa=parent_scientific_name)
        self.assertTrue(len(results_parent) == len(results_parentless))
        
    def test_hyla_pruned_subspecies(self, name='Hyla versicolor versicolor', parent_scientific_name='Chordata'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa=parent_scientific_name)
        scientific_names = [ref.scientific_name for ref in refs]
        self.assertIn('Hyla versicolor versicolor', scientific_names)
    
    def test_fuzzy_canis_lupus(self, name='Canisse lupus', parent_scientific_name='Chordata'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa=parent_scientific_name)
        scientific_names = [ref.scientific_name for ref in refs]
        self.assertIn('Canis lupus', scientific_names)
        
    def test_fuzzy_canis_lupus_cdpnq(self, name='Canisse lupus'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        cdpnq_ref = [ref for ref in refs if ref.source_name == 'CDPNQ' and ref.is_parent == False ][0]
        self.assertFalse(cdpnq_ref.match_type == 'exact')

    def test_fuzzy_invalid(self, name='Hyla versicolor versicool'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        scientific_names = [ref.scientific_name for ref in refs]
        self.assertIn('Hyla versicolor versicolor', scientific_names)
        
    def test_gbif_match_type(self, name='Libellula julia', valid_synonym='Ladona julia'):
        results = taxa_ref.TaxaRef._from_gbif_singleton(name)
        match_rec = [ref for ref in results if ref.scientific_name == name][0]
        synonym_rec = [ref for ref in results if ref.scientific_name == valid_synonym][0]
        self.assertTrue(match_rec.match_type == 'exact')
        self.assertTrue(synonym_rec.match_type == None)
        
    def test_gn_match_type(self, name='Libellula julia', valid_synonym='Ladona julia'):
        results = taxa_ref.TaxaRef.from_global_names(name)
        match_rec = [ref for ref in results if ref.scientific_name == name][0]
        synonym_rec = [ref for ref in results if ref.scientific_name == valid_synonym][0]
        self.assertTrue(match_rec.match_type == 'exact')
        self.assertTrue(synonym_rec.match_type == None)
        
    def test_from_all_sources_match_type(self, name='Libellula julia', valid_synonym='Ladona julia'):
        results = taxa_ref.TaxaRef.from_all_sources(name)
        match_recs = [ref for ref in results if ref.scientific_name == name]
        synonym_recs = [ref for ref in results if ref.scientific_name == valid_synonym]
        self.assertTrue(all([rec.match_type == 'exact' for rec in match_recs]))
        self.assertTrue(all([rec.match_type == None for rec in synonym_recs]))
        
    def test_cyprinus_carpio_match_type(self, name='Cyprinus carpio', authorship='Linnaeus 1758'):
        results = taxa_ref.TaxaRef.from_all_sources(name, authorship)
        wrong_match = [ref for ref in results if (ref.match_type == 'partialexact' or ref.match_type == 'exact')
                       and ref.rank == 'genus']
        self.assertTrue(len(wrong_match) == 0)

    def test_calliope_match_type(self, name='Calliope calliope', authorship='(Pallas, 1776)'):
        results = taxa_ref.TaxaRef.from_all_sources(name, authorship)
        matches =  [ref for ref in results if (ref.match_type == 'partialexact' and ref.is_parent == False)]
        self.assertTrue(len(matches) == 0)

    def test_gbif_catharus_swainsoni(self, name='Catharus swainsoni', authorship='(Tschudi, 1845)', parent_taxa='Chordata'):
        results = taxa_ref.TaxaRef.from_all_sources(name, authorship, parent_taxa)
        self.assertTrue(
            any(res.scientific_name == 'Catharus ustulatus swainsoni' and
                res.source_name == 'GBIF Backbone Taxonomy' for res in results)
        )
        
    def test_cod_cdpnq_no_doublematch(self, name='Gadus ogac', authorship='Tilesius, 1810', parent_taxa=''):
        results = taxa_ref.TaxaRef.from_all_sources(name, authorship, parent_taxa)
        self.assertTrue(
            not any(res.scientific_name == 'Gadus macrocephalus' and
                    res.source_name == 'CDPNQ' for res in results)
        )

class TestComplex(unittest.TestCase):
    # Test for Myotis complex entries from CDPNQ
    def test_cdpnq_complex_myotis(self, name='Myotis lucifugus|Myotis septentrionalis|Myotis leibii'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        cdpnq_refs = [ref for ref in refs if ref.source_name == 'CDPNQ' and ref.match_type == 'complex']
        self.assertEqual(len(cdpnq_refs), 3)

    # Test for Lasiurus complex entries from CDPNQ
    def test_cdpnq_complex_lasiurus(self, name='Lasiurus cinereus|Lasionycteris noctivagans'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        cdpnq_refs = [ref for ref in refs if ref.source_name == 'CDPNQ' and ref.match_type == 'complex']
        self.assertEqual(len(cdpnq_refs), 2)

    def test_complex_is_true(self,
                             name='Lasiurus cinereus|Lasionycteris noctivagans'):
        out = taxa_ref.is_complex(name)
        self.assertTrue(out)

    def test_complex_is_false(self, name='Lasionycteris noctivagans'):
        out = taxa_ref.is_complex(name)
        self.assertFalse(out)

    def test_from_all_sources(self,
                              name='Lasiurus cinereus|Lasionycteris noctivagans'):

        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) > 1)

        is_match_complex = [ref.match_type == "complex" for ref in refs]
        self.assertTrue(
            any(is_match_complex) and not all(is_match_complex)
        )
        is_common_parent = [ref.match_type ==
                            "complex_closest_parent" for ref in refs]
        self.assertTrue(
            any(is_common_parent) and not all(is_common_parent)
        )

        distinct_srid = {(r.source_id, r.source_record_id) for r in refs}
        self.assertTrue(
            len(refs) == len(distinct_srid)
        )

        # BUG: For a single source, all but one match_type is complex
        # Assert that all sources have at least one null match_type, which is for a parent to the complex_closest_parent
        # 2023-03-03: This tests is no longer valid with CDPNQ refs implementation

        # BUG: Complex_closest_parent for wrong ref
        source_names = list({ref.source_name for ref in refs})
        source_max_rank = {
            src: max(
                [ref.rank_order for ref in refs if src == ref.source_name])
            for src in source_names}
        source_match_rank = {src: ref.rank_order
                             for src in source_names
                             for ref in refs
                             if ref.match_type == "complex_closest_parent"}
        self.assertFalse(
            any(
                [source_max_rank[src] == source_match_rank[src]
                    for src in source_names]))

        # BUG: Only one valid ref from GBIF
        gbif_refs = [
            ref for ref in refs
            if ref.source_name == 'GBIF Backbone Taxonomy'
                and ref.valid
                and ref.match_type == "complex"
                and ref.rank == "species"
        ]
        self.assertTrue(len(gbif_refs) == 2)

    # def test_from_global_names(self, name = 'formica querquetulana', authorship = 'Kennedy & Davis, 1937'):
    #     refs = taxa_ref.TaxaRef.from_global_names(name, authorship)
    #     [self.assertTrue(v) for ref in refs for k, v in vars(ref).items()
    #     if k not in ['id', 'match_type', 'authorship', 'rank_order']
    #     ]
    #     self.assertTrue(any([ref.match_type for ref in refs]))
    #     self.assertTrue(any([ref.authorship for ref in refs]))
    #     self.assertTrue(all([isinstance(ref.rank_order, int) for ref in refs]))

    # Test bug case for name with hyphen
    def test_from_all_sources_hyphen(self, name='Ptilium crista-castrensis'):
        refs = taxa_ref.TaxaRef.from_all_sources(name)
        self.assertTrue(len(refs) >= 1)

class TestParent(unittest.TestCase):
    # Test case for Salix matching for a genus of Animalia and a genus of Plantae
    def test_from_all_sources_parent_taxa_salix(self, name='Salix', parent_taxa = 'Plantae'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa = parent_taxa)
        
        # Records with rank == 'kingdom'
        kingdom_name = {ref.scientific_name for ref in refs if ref.rank == 'kingdom'}

        # Assert only one kingdom name
        self.assertEqual(len(kingdom_name), 1)

        # Assert any is genus
        self.assertTrue(any([ref.rank == 'genus' for ref in refs]))

        # Assert any is from VASCAN
        self.assertTrue(any([ref.source_name == 'VASCAN' for ref in refs]))

    # Test case for Rangifer tarandus where no existing conflict is known
    def test_from_all_sources_parent_taxa_rangifer(self, name='Rangifer tarandus', parent_taxa = 'Mammalia'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa = parent_taxa)
        
        # Records with rank == 'kingdom'
        kingdom_name = {ref.scientific_name for ref in refs if ref.rank == 'kingdom'}

        # Assert only one kingdom name
        self.assertEqual(len(kingdom_name), 1)

        # Assert CDPNQ is in the sources
        self.assertTrue(any([ref.source_name == 'CDPNQ' for ref in refs]))        
        

    # Test case for bad parent_taxa
    def test_from_all_sources_parent_taxa_rangifer_bad_match(self, name='Rangifer tarandus', parent_taxa = 'Plantae'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa = parent_taxa)

        # Assert no refs
        self.assertFalse(refs)


    # Bug : Bad match are kept at the match level
    def test_from_all_sources_parent_taxa_salix_animalia(self, name='Salix', parent_taxa = 'Animalia'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa = parent_taxa)

        # Assert Only one match at the species level for Catalogue of Life
        col_genus_refs = [ref for ref in refs if ref.source_name == 'Catalogue of Life' and ref.rank == 'genus']
        self.assertEqual(len(col_genus_refs), 1)



    # Test case using parent_taxa with complex
    def test_from_all_sources_parent_taxa_complex(self, name='Lasiurus cinereus|Lasionycteris noctivagans', parent_taxa = 'Mammalia'):
        refs = taxa_ref.TaxaRef.from_all_sources(name, parent_taxa = parent_taxa)
        self.assertTrue(len(refs) > 1)

        is_match_complex = [ref.match_type == "complex" for ref in refs]
        self.assertTrue(
            any(is_match_complex) and not all(is_match_complex)
        )
        is_common_parent = [ref.match_type ==
                            "complex_closest_parent" for ref in refs]
        self.assertTrue(
            any(is_common_parent) and not all(is_common_parent)
        )

        distinct_srid = {(r.source_id, r.source_record_id) for r in refs}
        self.assertTrue(
            len(refs) == len(distinct_srid)
        )

        # Assert any ref is from CDPNQ
        self.assertTrue(any([ref.source_name == 'CDPNQ' for ref in refs]))


if __name__ == '__main__':
    unittest.main()