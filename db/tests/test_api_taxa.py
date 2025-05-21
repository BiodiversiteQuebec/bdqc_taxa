from config import connect, DB_CONFIG
import pandas as pd
import unittest

class TestTaxa(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    # Connection test moved to test_db_connection.py

    def test_get_taxa(self):#
        self.cur.execute("SELECT * FROM api.taxa LIMIT 1")
        result = self.cur.fetchone()

        # Assert data in row
        self.assertIsNotNone(result)

    def test_synonym_scientific_name_dryobates_villosus(self, observed_scientific_name='Dryobates villosus', valid_scientific_name='Dryobates villosus'):
        self.cur.execute(f"SELECT valid_scientific_name, valid_scientific_name = '{valid_scientific_name}' AS test_pass FROM api.taxa WHERE observed_scientific_name = '{observed_scientific_name}'")
        result = self.cur.fetchone()
        self.assertEqual(result[0], 'Dryobates villosus')
        self.assertTrue(result[1])

    def test_valid_scientific_name_leuconotopicus_villosus(self, observed_scientific_name='Leuconotopicus villosus', valid_scientific_name='Dryobates villosus'):
        self.cur.execute(f"SELECT valid_scientific_name, valid_scientific_name = '{valid_scientific_name}' AS test_pass FROM api.taxa WHERE observed_scientific_name = '{observed_scientific_name}'")
        result = self.cur.fetchone()
        self.assertEqual(result[0], 'Dryobates villosus')
        self.assertTrue(result[1])

    def test_valid_scientific_name_leuconotopicus_villosus_septentrionalis(self):
        self.cur.execute("SELECT valid_scientific_name, valid_scientific_name = 'Leuconotopicus villosus septentrionalis' AS test_pass FROM api.taxa WHERE observed_scientific_name = 'Leuconotopicus villosus septentrionalis'")
        result = self.cur.fetchone()
        self.assertEqual(result[0], 'Leuconotopicus villosus septentrionalis')
        self.assertTrue(result[1])

    def test_valid_scientific_name_dendrocopos_villosus(self):
        self.cur.execute("SELECT valid_scientific_name, valid_scientific_name = 'Dryobates villosus' AS test_pass FROM api.taxa WHERE observed_scientific_name = 'Dendrocopos villosus'")
        result = self.cur.fetchone()
        self.assertEqual(result[0], 'Dryobates villosus')
        self.assertTrue(result[1])

    def test_valid_scientific_name_same_than_species(self):
        query = """
            select * from api.taxa
            where valid_scientific_name != species
            and rank = 'species'
        """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(len(df), 0)

    def test_same_vernacular_for_same_valid_scientific_name(self):
        query = """
            SELECT *
            FROM api.taxa t
            WHERE t.valid_scientific_name IN (
                SELECT valid_scientific_name
                FROM api.taxa
                GROUP BY valid_scientific_name
                HAVING COUNT(DISTINCT vernacular_en) > 1
            )
        """
        df = pd.read_sql(query, self.conn)
        self.assertEqual(len(df), 0)    


class TestObsRefpreferredLookup(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()
    
    def test_all_distinct(self):
        query = """
            SELECT
                count(distinct(id_taxa_ref, id_taxa_obs)) as count_preferred_rows,
                count(*) as count_all_rows
            FROM rubus.taxa_obs_ref_preferred
        """
        self.cur.execute(query)
        result = self.cur.fetchone()
        self.assertTrue(result[0])

# Normal that this test fails as a couple of taxa_obs do not get parsed in the pipeline
    def test_all_taxa_obs_in_preferred(self):
        query = """
            SELECT
                distinct on (id_taxa_obs)
                lu.*
                from rubus.taxa_obs_ref_lookup lu
                where id_taxa_obs not in (SELECT id_taxa_obs from rubus.taxa_obs_ref_preferred)
            """
        results = pd.read_sql(query, self.conn)
        # Assert all taxa_obs in preffered lookup
        self.assertEqual(len(results), 0)

    def test_all_taxa_obs_has_match(self):
        query = """
            SELECT
                count(distinct(id_taxa_obs)) as taxa_obs_count,
                SUM(CASE WHEN is_match IS TRUE THEN 1 ELSE 0 END) as is_match_count
            FROM rubus.taxa_obs_ref_preferred
        """
        self.cur.execute(query)
        result = self.cur.fetchone()

        # Assert difference between taxa_obs_count and is_match_count is less than 27
        # 27 is the number of taxa_obs that have no match
        self.assertLessEqual(result[0] - result[1], 27)

    def test_species_genus_same_root(self):
        query = """
            SELECT
                sp_lu.id_taxa_obs as id_taxa_obs,
                sp_ref.scientific_name as species_name,
                ge_ref.scientific_name as genus_name,
                sp_ref.source_name as species_source_name,
                ge_ref.source_name as genus_source_name
            FROM rubus.taxa_obs_ref_preferred as sp_lu
            JOIN rubus.taxa_obs_ref_preferred as ge_lu ON sp_lu.id_taxa_obs = ge_lu.id_taxa_obs
                AND sp_lu.rank = 'species' AND ge_lu.rank = 'genus'
            JOIN rubus.taxa_ref as sp_ref ON sp_lu.id_taxa_ref = sp_ref.id
            JOIN rubus.taxa_ref as ge_ref ON ge_lu.id_taxa_ref = ge_ref.id
        """
        df = pd.read_sql(query, self.conn)
        # Column genus strings are in species strings
        df['genus_in_species'] = df.apply(lambda x: x['genus_name'] in x['species_name'], axis=1)

        # Bad entries where genus is different from species
        bad_entries = df.loc[df['genus_in_species'] == False]
        self.assertEqual(len(bad_entries), 0)
    
    QUERY_TAXA_OBS_JOIN = f"""
        SELECT
            taxa_obs.id id_taxa_obs,
            taxa_obs.scientific_name observed_scientific_name,
            valid_ref.scientific_name,
            valid_ref.rank,
            valid_ref.source_name,
            valid_lu.is_match
        FROM taxa_obs
        LEFT JOIN rubus.taxa_obs_ref_preferred valid_lu ON taxa_obs.id = valid_lu.id_taxa_obs
        LEFT JOIN rubus.taxa_ref valid_ref ON valid_lu.id_taxa_ref = valid_ref.id
        WHERE is_match IS TRUE
        """
    
    def test_query_leuconotopicus_villosus(self,scientific_name='Leuconotopicus villosus',
                                           valid_scientific_name='Dryobates villosus',
                                           valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_JOIN + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        df = pd.read_sql(query, self.conn)

        # Remove duplicates on columns scientific_name and source_name
        df = df.drop_duplicates(subset=['scientific_name', 'source_name'])

        # Assert only one result
        self.assertEqual(len(df), 1)

        # Assert valid scientific name
        self.assertEqual(df['scientific_name'].values[0], valid_scientific_name)

        # Assert valid source name
        self.assertEqual(df['source_name'].values[0], valid_source_name)

    def test_query_picoides_villosus(self,scientific_name='Picoides villosus',
                                           valid_scientific_name='Dryobates villosus',
                                           valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_JOIN + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        df = pd.read_sql(query, self.conn)

        # Remove duplicates on columns scientific_name and source_name
        df = df.drop_duplicates(subset=['scientific_name', 'source_name'])

        # Assert only one result
        self.assertEqual(len(df), 1)

        # Assert valid scientific name
        self.assertEqual(df['scientific_name'].values[0], valid_scientific_name)

        # Assert valid source name
        self.assertEqual(df['source_name'].values[0], valid_source_name)


    def test_query_erethizon_dorsatum(self,scientific_name='Erethizon dorsatus',
                               valid_scientific_name='Erethizon dorsatum',
                               valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_JOIN + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        df = pd.read_sql(query, self.conn)

        # Remove duplicates on columns scientific_name and source_name
        df = df.drop_duplicates(subset=['scientific_name', 'source_name'])

        # Assert only one result
        self.assertEqual(len(df), 1)

        # Assert valid scientific name
        self.assertEqual(df['scientific_name'].values[0], valid_scientific_name)

        # Assert valid source name
        self.assertEqual(df['source_name'].values[0], valid_source_name)


    def test_rangifer_tarandus(self,scientific_name='Rangifer tarandus',
                               valid_scientific_name='Rangifer tarandus caribou',
                               valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_JOIN + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        df = pd.read_sql(query, self.conn)

        # Remove duplicates on columns scientific_name and source_name
        df = df.drop_duplicates(subset=['scientific_name', 'source_name'])

        # Assert only one result
        self.assertEqual(len(df), 1)

        # Assert valid scientific name
        self.assertEqual(df['scientific_name'].values[0], valid_scientific_name)

        # Assert valid source name
        self.assertEqual(df['source_name'].values[0], valid_source_name)

    QUERY_TAXA_REF_JOIN = f"""
        SELECT
            obs_ref.scientific_name observed_scientific_name,
            -- obs_ref.rank,
            -- obs_ref.source_name,
            valid_lu.id_taxa_obs as id_taxa_obs,
            valid_ref.scientific_name,
            valid_ref.rank,
            valid_ref.source_name,
            valid_lu.is_match
        FROM rubus.taxa_ref obs_ref
        LEFT JOIN rubus.taxa_obs_ref_lookup obs_lu ON obs_ref.id = obs_lu.id_taxa_ref and obs_lu.is_parent IS FALSE
        LEFT JOIN rubus.taxa_obs_ref_preferred valid_lu ON obs_lu.id_taxa_obs = valid_lu.id_taxa_obs
        LEFT JOIN rubus.taxa_ref valid_ref ON valid_lu.id_taxa_ref = valid_ref.id
        WHERE valid_lu.is_match IS TRUE
        """
        # TESTS TO BE DONE
        # -- CDPNQ refs = CDPNQ refs

    def test_vascan_taxa_ref_always_valid(self, source_name='VASCAN'):
        query = self.QUERY_TAXA_REF_JOIN + f"AND obs_ref.source_name = '{source_name}'"
        df = pd.read_sql(query, self.conn)

        # Assert all source names are from VASCAN
        self.assertTrue((df['source_name'] == source_name).all())
        
    def test_cdpnq_taxa_ref_always_valid(self, source_name='CDPNQ'):
        query = self.QUERY_TAXA_REF_JOIN + f"AND obs_ref.source_name = '{source_name}'"
        df = pd.read_sql(query, self.conn)

class TestTaxaRefVernacularLookup(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    QUERY_TAXA_OBS_VERNACULAR_LOOKUP = f"""
(
    SELECT
        taxa_obs.scientific_name AS observed_scientific_name,
        taxa_vernacular.id,
        taxa_vernacular.language,
        taxa_vernacular.name AS valid_vernacular_name,
        taxa_vernacular.rank AS valid_vernacular_rank,
        taxa_vernacular.source_name AS valid_vernacular_source_name,
        vern_lu.is_match
    FROM taxa_obs
    LEFT JOIN rubus.taxa_obs_ref_preferred ref_lu ON taxa_obs.id = ref_lu.id_taxa_obs
    LEFT JOIN rubus.taxa_ref_vernacular_preferred vern_lu ON ref_lu.id_taxa_ref = vern_lu.id_taxa_ref
    LEFT JOIN rubus.taxa_vernacular taxa_vernacular ON vern_lu.id_taxa_vernacular_en = taxa_vernacular.id
    WHERE vern_lu.is_match IS TRUE
      AND taxa_obs.scientific_name = %s
)
UNION
(
    SELECT
        taxa_obs.scientific_name AS observed_scientific_name,
        taxa_vernacular.id,
        taxa_vernacular.language,
        taxa_vernacular.name AS valid_vernacular_name,
        taxa_vernacular.rank AS valid_vernacular_rank,
        taxa_vernacular.source_name AS valid_vernacular_source_name,
        vern_lu.is_match
    FROM taxa_obs
    LEFT JOIN rubus.taxa_obs_ref_preferred ref_lu ON taxa_obs.id = ref_lu.id_taxa_obs
    LEFT JOIN rubus.taxa_ref_vernacular_preferred vern_lu ON ref_lu.id_taxa_ref = vern_lu.id_taxa_ref
    LEFT JOIN rubus.taxa_vernacular taxa_vernacular ON vern_lu.id_taxa_vernacular_fr = taxa_vernacular.id
    WHERE vern_lu.is_match IS TRUE
      AND taxa_obs.scientific_name = %s
)
"""    
    
    def test_query_actaea_alba(self,scientific_name='Actaea alba',
                               valid_vernacular_name='Actée à gros pédicelles',
                               valid_source_name='Database of Vascular Plants of Canada (VASCAN)'):
        query = self.QUERY_TAXA_OBS_VERNACULAR_LOOKUP
        df = pd.read_sql(query, self.conn, params = (scientific_name, scientific_name))
        #Assert french language in results
        self.assertTrue(df['language'].str.contains('fr').any())
        # Assert english language in results
        self.assertTrue(df['language'].str.contains('en').any())
        # Assert any source names are from VASCAN
        self.assertEqual(df['valid_vernacular_source_name'].unique()[0], valid_source_name)
        # Assert all source names are from VASCAN
        self.assertTrue((df['valid_vernacular_source_name'] == valid_source_name).all())
        # Assert all fr vernacular names are 'Actée à gros pédicelles'
        self.assertTrue((df[df['language'] == 'fr']['valid_vernacular_name'] == valid_vernacular_name).all())
            
    def test_query_leuconotopicus_villosus(self,scientific_name='Leuconotopicus villosus',
                                           valid_vernacular_name='Pic chevelu',
                                           valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_VERNACULAR_LOOKUP + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        df = pd.read_sql(query, self.conn)
        # self.assertEqual(len(results), 2)
        # Assert french language in results
        self.assertTrue(df['language'].str.contains('fr').any())
        # Assert english language in results
        self.assertTrue(df['language'].str.contains('en').any())
        # Assert any source names are from CDPNQ
        self.assertEqual(df['valid_vernacular_source_name'].unique()[0], valid_source_name)
        # Assert all source names are from CDPNQ
        self.assertTrue((df['valid_vernacular_source_name'] == valid_source_name).all())
        # Assert all fr vernacular names are 'Pic chevelu'
        self.assertTrue((df[df['language'] == 'fr']['valid_vernacular_name'] == valid_vernacular_name).all())

    def test_query_picoides_villosus(self,scientific_name='Picoides villosus',
                                           valid_source_name='CDPNQ'):
        query = self.QUERY_TAXA_OBS_VERNACULAR_LOOKUP + f"AND taxa_obs.scientific_name = '{scientific_name}'"
        self.cur.execute(query)
        result = self.cur.fetchone()
        self.assertEqual(result[5], valid_source_name)

    QUERY_TAXA_REF_VERNACULAR_LOOKUP = f"""
        SELECT
            obs_lu.id_taxa_obs as id_taxa_obs,
            taxa_ref.scientific_name observed_scientific_name,
            taxa_vernacular.id,
            taxa_vernacular.language,
            taxa_vernacular.name as valid_vernacular_name,
            taxa_vernacular.rank as valid_vernacular_rank,
            taxa_vernacular.source_name as valid_vernacular_source_name,
            lu.is_match
        FROM rubus.taxa_ref
        LEFT JOIN rubus.taxa_obs_ref_lookup obs_lu ON taxa_ref.id = obs_lu.id_taxa_ref and obs_lu.is_parent IS FALSE
        LEFT JOIN rubus.taxa_obs_vernacular_preferred lu ON obs_lu.id_taxa_obs = lu.id_taxa_obs
        LEFT JOIN rubus.taxa_vernacular taxa_vernacular ON lu.id_taxa_vernacular = taxa_vernacular.id
        WHERE lu.is_match IS TRUE
        """    

    def test_all_vascan(self, source_name_ref='VASCAN', source_name_vernacular='Database of Vascular Plants of Canada (VASCAN)'):
        query = self.QUERY_TAXA_REF_VERNACULAR_LOOKUP + f"AND taxa_ref.source_name = '{source_name_ref}'"
        df = pd.read_sql(query, self.conn)
        # Assert all source names are from VASCAN
        self.assertTrue((df['valid_vernacular_source_name'] == source_name_vernacular).all())
        
    def test_all_cdpnq(self, source_name='CDPNQ', source_rank='species'):
        query = self.QUERY_TAXA_REF_VERNACULAR_LOOKUP + f"AND taxa_ref.source_name = '{source_name}'"
        query += f"AND taxa_ref.rank = '{source_rank}'"
        df = pd.read_sql(query, self.conn)
        # Assert all fr source names are from CDPNQ
        fra_df = df.loc[df['language'] == 'fra']
        self.assertTrue((fra_df['valid_vernacular_source_name'] == source_name).all())

if __name__ == '__main__':
    unittest.main()
