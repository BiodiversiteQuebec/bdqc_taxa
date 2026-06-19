from config import connect, DB_CONFIG
import pandas as pd
import unittest

class TestMatchTaxa(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()
        
    QUERY_MATCH_TAXA = f"""
        SELECT * FROM api.match_taxa (%s)
        """
        
    def test_match_taxa_canis_lupus(self, taxa_name='Canis lupus'):
        query = self.QUERY_MATCH_TAXA
        df = pd.read_sql(query, self.conn, params = (taxa_name,))
        self.assertTrue(not df['observed_scientific_name'].str.contains('latrans').any())        # check that there is no match at the rank = 'genus'
        self.assertTrue('genus' not in df['rank'].values)

    def test_match_taxa_all_canis(self, taxa_name='Canis'):
        query = self.QUERY_MATCH_TAXA
        df = pd.read_sql(query, self.conn, params = (taxa_name,))
        allowed = {'Canis', 'Canis lupus', 'Canis latrans', 'Canis lycaon'}        
        self.assertTrue(set(df['valid_scientific_name'].unique()).issubset(allowed))
        
    def test_dryobates_villosus_all_synonyms(self, taxa_name='Dryobates villosus'):
        query = self.QUERY_MATCH_TAXA
        df = pd.read_sql(query, self.conn, params = (taxa_name,))
        required = {'Dryobates villosus', 'Picoides villosus', 'Picoides villosus villosus',
                   'Picoides villosus septentrionalis', 'Dendrocopos villosus',
                   'Dendrocopos villosus villosus', 'Leuconotopicus villosus',
                   'Leuconotopicus villosus septentrionalis', 'Leuconotopicus villosus villosus',
                   'dryobates villosus'}
        self.assertTrue(set(df['observed_scientific_name'].unique()).issubset(required))

    def test_poa_nemoralis_only(self, taxa_name='Poa nemoralis'):
        query = self.QUERY_MATCH_TAXA
        df_poa_nemoralis = pd.read_sql(query, self.conn, params = (taxa_name,))
        self.assertTrue((df_poa_nemoralis['valid_scientific_name'] == 'Poa nemoralis').all())
        
    def test_rhamnus_only(self, taxa_name='Rhamnus'):
        query = self.QUERY_MATCH_TAXA
        df_rhamnus = pd.read_sql(query, self.conn, params = (taxa_name,))
        self.assertTrue(df_rhamnus['valid_scientific_name'].str.contains('Rhamnus').all())

#    Deprecated test: We no longer match synonyms as only valid scientific names are thrown in match_taxa, 
#    so this test is no longer relevant.
#    def test_match_taxa_Picoides_villosus_Dendrocopos_villosus(self):
#       pass

#   Deprecated test: We no longer match synonyms as only valid scientific names are thrown in match_taxa, 
#   so this test is no longer relevant.
#    def test_match_taxa_Picoides_villosus_villosus_no_subsp(self, taxa_name='Picoides villosus villosus'):
#        pass
