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
        allowed = {'Canis', 'Canis lupus', 'Canis latrans'}        
        self.assertTrue(set(df['valid_scientific_name'].unique()).issubset(allowed))
        
        
    def test_match_taxa_Picoides_villosus_Dendrocopos_villosus(self):
        query = self.QUERY_MATCH_TAXA
        df_picoides = pd.read_sql(query, self.conn, params = ('Picoides villosus',))
        self.assertTrue('Dryobates villosus' in df_picoides['observed_scientific_name'].values)
        self.assertTrue('Leuconotopicus villosus' in df_picoides['observed_scientific_name'].values)
        self.assertTrue(df_picoides['valid_scientific_name'].unique() == 'Dryobates villosus')
        
        df_dendrocopos = pd.read_sql(query, self.conn, params = ('Dendrocopos villosus',))
        self.assertTrue('Dryobates villosus' in df_dendrocopos['observed_scientific_name'].values)
        self.assertTrue('Leuconotopicus villosus' in df_dendrocopos['observed_scientific_name'].values)
        self.assertTrue(df_dendrocopos['valid_scientific_name'].unique() == 'Dryobates villosus')

    def test_match_taxa_Picoides_villosus_villosus_no_subsp(self, taxa_name='Picoides villosus villosus'):
        query = self.QUERY_MATCH_TAXA
        df = pd.read_sql(query, self.conn, params = (taxa_name,))
        self.assertTrue('subspecies' not in df['rank'].values)

    def test_poa_nemoralis_only(self, taxa_name='Poa nemoralis'):
        query = self.QUERY_MATCH_TAXA
        df_poa_nemoralis = pd.read_sql(query, self.conn, params = (taxa_name,))
        self.assertTrue((df_poa_nemoralis['valid_scientific_name'] == 'Poa nemoralis').all())