from config import connect, DB_CONFIG
import pandas as pd
import unittest

class TestVernacularFromRef(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    # This test should fail. It needs authorship in table rubus.taxa_ref for genus-level matches to work, wchich is not the case currently.
    # Arenaria is a relevant test case because it matches distinct plantae (Sandwort) and animalia (Turnstones) genera.
    def test_genus_no_match_without_authorship(self, scientific_name='Arenaria', rank='genus', source_name='GBIF Backbone Taxonomy',
                                               expected_vernaculars=['Sandwort', 'Turnstones']):

        query = """
        SELECT v.*
        FROM rubus.taxa_ref r
        CROSS JOIN LATERAL rubus.taxa_vernacular_from_match(r.scientific_name, r.authorship, r.rank) AS v
        WHERE r.scientific_name = %s AND r.rank = %s AND r.source_name = %s;
        """
        df = pd.read_sql_query(query, self.conn, params=(scientific_name, rank, source_name))
        
        # Assert all expected vernaculars are present from source_name
        gbif_df = df.loc[df['source'] == source_name]
        
        self.assertTrue(all(vern in gbif_df['name'].values for vern in expected_vernaculars))
        
    # Test for siingle ambiguous genus: Arenaria plant
    def test_genus_match_with_ambiguous_plantae(self, scientific_name='Arenaria', authorship='L.', rank='genus',
                                       expected_vernacular='Sabline', language='fra', source_name='GBIF Backbone Taxonomy'):

        query = """
        SELECT v.*
        FROM rubus.taxa_vernacular_from_match(%s, %s, %s) AS v
        """
        df = pd.read_sql_query(query, self.conn, params=(scientific_name, authorship, rank))
        
        # Assert all results in language has expected vernacular
        lang_df = df.loc[df['language'] == language]
        self.assertTrue((lang_df['name'] == expected_vernacular).all())
  
    # Test for siingle ambiguous genus: Arenaria animal
    def test_genus_match_with_ambiguous_animalia(self, scientific_name='Arenaria', authorship='Brisson, 1760', rank='genus',
                                       expected_vernacular='Turnstones', language='eng', source_name='GBIF Backbone Taxonomy'):

        query = """
        SELECT v.*
        FROM rubus.taxa_vernacular_from_match(%s, %s, %s) AS v
        """
        df = pd.read_sql_query(query, self.conn, params=(scientific_name, authorship, rank))
        
        # Assert all results in language has expected vernacular
        lang_df = df.loc[df['language'] == language]
        self.assertTrue((lang_df['name'] == expected_vernacular).all())

if __name__ == '__main__':
    unittest.main()
