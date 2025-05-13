from config import connect
import unittest
import pandas as pd

# Should return records with match_type not null for source CDPNQ
# {
#   "id": 161092,
#   "scientific_name": "Castor canadensis canadensis",
#   "authorship": "",
#   "parent_scientific_name": "Chordata"
# }

# {
#   "id": 1144047,
#   "scientific_name": "Catharus minimus bicknelli",
#   "authorship": "(Ridgway, 1882)",
#   "rank": "subspecies",
#   "parent_scientific_name": "Chordata"
# }

# Should return records with match_type null for source Bryoquel
# {
#   "id": 120628,
#   "scientific_name": "Sphagnum robustum",
#   "authorship": "(Warnst.) Röll",
#   "rank": "species",
#   "parent_scientific_name": "Bryophyta"
# }

class TestTaxaMatchSources(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    def test_taxa_match_sources_cdpnq_no_match_castorg(self, scientific_name="Castor canadensis canadensis",
                                                   authorship="", parent_scientific_name="Chordata",
                                                   source_name="CDPNQ"):
        query = f"""
            SELECT *
            FROM match_taxa_sources(
                        name=>%s,
                        name_authorship=>%s,
                        parent_scientific_name=>%s)
            WHERE source_name = %s;
        """
        df = pd.read_sql(query, self.conn, params=(scientific_name, authorship, parent_scientific_name, source_name))
        
        # Should return at least one record with match_type not null
        self.assertTrue(df['match_type'].notnull().any())

    def test_taxa_match_sources_cdpnq_no_match_bug_bicknelli(self, scientific_name="Catharus minimus bicknelli",
                                                    authorship="(Ridgway, 1882)", parent_scientific_name="Chordata",
                                                    source_name="CDPNQ"):
          query = f"""
                SELECT *
                FROM match_taxa_sources(
                            name=>%s,
                            name_authorship=>%s,
                            parent_scientific_name=>%s)
                WHERE source_name = %s;
          """
          df = pd.read_sql(query, self.conn, params=(scientific_name, authorship, parent_scientific_name, source_name))
          
          # Should return at least one record with match_type not null
          self.assertTrue(df['match_type'].notnull().any())

    def test_taxa_match_sources_bryoquel_match_sphagrob(self, scientific_name="Sphagnum robustum",
                                                    authorship="(Warnst.) Röll", parent_scientific_name="Bryophyta",
                                                    source_name="Bryoquel"):
          query = f"""
                SELECT *
                FROM match_taxa_sources(
                            name=>%s,
                            name_authorship=>%s,
                            parent_scientific_name=>%s)
                WHERE source_name = %s;
          """
          df = pd.read_sql(query, self.conn, params=(scientific_name, authorship, parent_scientific_name, source_name))
          
          # Should return at least one record with match_type null
          self.assertTrue(df['match_type'].isnull().all())