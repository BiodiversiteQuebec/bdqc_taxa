# test_wikidata.py

import unittest

from bdqc_taxa import wikidata

# Test case : chiroptera, Q28425


class TestWikidata(unittest.TestCase):
    def test_search_entities(self, query='chiroptera'):
        searches = wikidata.search_entities(query)
        # Assert there is at least one result
        self.assertGreater(len(searches), 0)
        # Assert dict contains field 'id'
        self.assertIn('id', searches[0])

    def test_get_entities(self, id='Q28425'):
        entities = wikidata.get_entities(id)
        # Assert only one result
        self.assertEqual(len(entities), 1)

        # Assert dict contains fields :'aliases', 'descriptions', 'labels', 'claims', 'sitelinks'
        entity = entities[0]
        self.assertIn('aliases', entity)
        self.assertIn('descriptions', entity)
        self.assertIn('labels', entity)
        self.assertIn('claims', entity)
        self.assertIn('sitelinks', entity)

    def test_get_entities_error(self, id='Q284ASDFAWE25'):
        with self.assertRaises(Exception):
            entity = wikidata.get_entities(id, languages=['fr'])

    def test_get_taxa_rank_entities(self):
        entities:dict = wikidata._get_taxa_rank_entities()
        # Assert keys 'species', 'genus', 'family', 'order', 'class', 'phylum' values starts with 'Q'
        self.assertIn('species', entities)
        self.assertTrue(entities['species'].startswith('Q'))
        self.assertIn('genus', entities)
        self.assertTrue(entities['genus'].startswith('Q'))
        self.assertIn('family', entities)
        self.assertTrue(entities['family'].startswith('Q'))
        self.assertIn('order', entities)
        self.assertTrue(entities['order'].startswith('Q'))
        self.assertIn('class', entities)
        self.assertTrue(entities['class'].startswith('Q'))
        self.assertIn('phylum', entities)
        self.assertTrue(entities['phylum'].startswith('Q'))
        self.assertIn('kingdom', entities)
        self.assertTrue(entities['kingdom'].startswith('Q'))