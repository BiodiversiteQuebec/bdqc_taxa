from config import connect
import unittest
import pandas as pd


class TestFixTaxa(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    def test_taxa_canis_latrans_no_canis_lupus(self):
        query = f"""
            SELECT
                matched_ref.*,
                taxa_obs.*,
                ref_lookup.match_type
            FROM rubus.taxa_ref AS matched_ref
            JOIN rubus.taxa_obs_ref_lookup AS ref_lookup
            ON matched_ref.id = ref_lookup.id_taxa_ref
            JOIN taxa_obs
            ON taxa_obs.id = ref_lookup.id_taxa_obs
            WHERE matched_ref.scientific_name ILIKE 'Canis lupus'
                and taxa_obs.scientific_name ilike '%latrans'
                and ref_lookup.match_type not in ('complex')
                and match_type is not null;
        """
        df = pd.read_sql(query, self.conn)
        
        # Should return no rows
        self.assertTrue(df.shape[0] == 0)