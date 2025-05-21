from config import connect, DB_CONFIG
import pandas as pd
import unittest

class TestTaxaGroupMembers(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.rollback()
        self.cur.close()
        self.conn.close()

    def test_taxa_group_members_in_taxa_ref(self, excepted_taxa=['Viruses']):
        # We want to make sure that all taxa defined by groups are observed and in ref
        query = f"""
            select
                taxa_obs.*,
                id_taxa_obs in (select id_taxa_obs from rubus.taxa_obs_ref_lookup) in_taxa_ref
            from rubus.taxa_group_members members
            join taxa_obs on members.id_taxa_obs = taxa_obs.id
            where taxa_obs.scientific_name not in ({','.join([f"'{taxon}'" for taxon in excepted_taxa])})
            """
        df = pd.read_sql(query, self.conn)
        # Assert in_taxa_ref = False count is 0
        self.assertEqual(0, df[df['in_taxa_ref'] == False].shape[0])

    def test_insert_taxa_obs_group_member(self, short_group = 'AFRICA', scientific_name = 'Panthera leo'):
        # Test the insert_taxa_obs_group_member function from migration
        # Insert Panthera leo with short='Africa' and other fields null/default
        self.cur.execute("""
            SELECT rubus.insert_taxa_obs_group_member(%s, %s)
        """, (short_group, scientific_name))

        # Check that the record exists in taxa_group_members
        self.cur.execute("""
            SELECT short, taxa_obs.scientific_name, id_taxa_obs
            FROM rubus.taxa_group_members
            JOIN taxa_obs ON rubus.taxa_group_members.id_taxa_obs = taxa_obs.id
            WHERE short = %s AND taxa_obs.scientific_name = %s
        """, (short_group, scientific_name))
        row = self.cur.fetchone()
        self.assertIsNotNone(row, "Panthera leo not inserted in taxa_group_members")
        self.assertEqual(row[0], short_group)
        self.assertIsNotNone(row[2], "id_taxa_obs should not be null for Panthera leo")

        # Check that the record exists in taxa_obs
        self.cur.execute("""
            SELECT id, scientific_name
            FROM taxa_obs
            WHERE scientific_name = %s
        """, (scientific_name,))
        row = self.cur.fetchone()
        self.assertIsNotNone(row, "Panthera leo not inserted in taxa_obs")
        self.assertEqual(row[1], scientific_name)
        self.assertIsNotNone(row[0], "id should not be null for Panthera leo")

        # Check that the record exists in taxa_obs_ref_lookup
        self.cur.execute("""
            SELECT id_taxa_obs, scientific_name
            FROM rubus.taxa_obs_ref_lookup
            JOIN taxa_obs ON taxa_obs_ref_lookup.id_taxa_obs = taxa_obs.id
            WHERE scientific_name = %s
        """, (scientific_name,))
        row = self.cur.fetchone()
        self.assertIsNotNone(row, "Panthera leo not inserted in taxa_obs_ref_lookup")



class TestTaxaObsGroupLookup(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.rollback()
        self.cur.close()
        self.conn.close()

    def test_taxa_obs_species_match_level_1_group(
            self, verified_taxa='Leuconotopicus villosus',
            level_1_group_short = 'BIRDS'):
        # Inject test data into taxa_obs and taxa_ref
        self.cur.execute("""
            INSERT INTO taxa_obs (scientific_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (verified_taxa,))

        self.cur.execute("""
            SELECT rubus.insert_taxa_ref_from_taxa_obs(id, scientific_name)
            FROM taxa_obs
            WHERE scientific_name = %s
        """, (verified_taxa,))

        # We want to make sure that verified taxa_obs is matched to taxa_obs_group_lookup
        query = f"""
            select
                taxa_obs.*,
                taxa_obs_group_lookup.*
            from rubus.taxa_obs_group_lookup_level_1_2_view taxa_obs_group_lookup
            join taxa_obs on taxa_obs_group_lookup.id_taxa_obs = taxa_obs.id
            where taxa_obs.scientific_name = '{verified_taxa}'
            and taxa_obs_group_lookup.short_group = '{level_1_group_short}'
            """
        df = pd.read_sql(query, self.conn)

        # Assert that the taxa_obs_group_lookup is not empty
        self.assertFalse(df.empty, f"Taxa {verified_taxa} not found in taxa_obs_group_lookup for group {level_1_group_short}")


    # DATA: taxa_obs_group_members (short, scientific_name)
    # INVASIVE_SPECIES, Phragmites australis subsp. australis

    def test_taxa_obs_group_lookup_matches_synonym_raynoutria(
            self, short_group='INVASIVE_SPECIES',
            taxa_obs_data = [
            (945215, "Reynoutria japonica", "Houtt.", "species", "Tracheophyta"),
            (157789, "Reynoutria japonica var. japonica","","variety","Tracheophyta"),
            (1000711, "Fallopia japonica", "(Houtt.) Ronse Decr.", "species", "Tracheophyta"),
            (132968, "Fallopia japonica var. japonica", "", "variety", "Tracheophyta"),
            (973663, "Polygonum cuspidatum", "Siebold & Zucc.", "species", "Tracheophyta"),
        ]):
        # Insert taxa_obs records for the synonyms (using provided data)
        for rec in taxa_obs_data:
            self.cur.execute("""
                INSERT INTO taxa_obs (id, scientific_name, authorship, rank, parent_scientific_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, rec)

        # Insert taxa_ref for each
        for rec in taxa_obs_data:
            self.cur.execute("""
                SELECT rubus.insert_taxa_ref_from_taxa_obs(id, scientific_name, authorship, parent_scientific_name)
                FROM taxa_obs
                where id = %s
                    and id not in (select id_taxa_obs from rubus.taxa_obs_ref_lookup where match_type is not null)
            """, (rec[0],))

        # Insert group member defined by reference list
        self.cur.execute("""
            SELECT rubus.insert_taxa_obs_group_member(%s, %s, %s, %s, %s)
        """, (short_group, taxa_obs_data[0][1], taxa_obs_data[0][2],  taxa_obs_data[0][3],  taxa_obs_data[0][4]))


        # Assert id_taxa_obs are related to group in taxa_obs_group_lookup
        query = f"""
            select *
            from rubus.taxa_obs_group_lookup_level_1_2_view glu
            join taxa_obs on glu.id_taxa_obs = taxa_obs.id
            where glu.short_group = '{short_group}'
            """
        result = pd.read_sql(query, self.conn)

        synonym_taxa_obs = [rec[1] for rec in taxa_obs_data]
        group_taxa_obs = [taxon for taxon in synonym_taxa_obs if taxon in result['scientific_name'].values]

        # Assert all observed names are in the group
        self.assertEqual(len(synonym_taxa_obs), len(group_taxa_obs))

    def test_taxa_obs_group_lookup_matches_synonym_phragmites(
            self, short_group='INVASIVE_SPECIES',
            taxa_obs_data = [
                (940523, "Phragmites australis", "(Cav.) Trin. ex Steud.", "species", "Tracheophyta"),
                (940821, "Phragmites australis subsp. americanus", "Saltonst., P.M.Peterson & Soreng", "subspecies", "Tracheophyta"),
                (957829, "Phragmites australis subsp. australis", "", "subspecies", "Tracheophyta"),
                (159407, "Phragmites communis", "Trin.", "species", "Tracheophyta"),
        ]):
        # Insert taxa_obs records for the synonyms (using provided data)
        for rec in taxa_obs_data:
            self.cur.execute("""
                INSERT INTO taxa_obs (id, scientific_name, authorship, rank, parent_scientific_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, rec)

        # Insert taxa_ref for each
        for rec in taxa_obs_data:
            self.cur.execute("""
                SELECT rubus.insert_taxa_ref_from_taxa_obs(id, scientific_name, authorship, parent_scientific_name)
                FROM taxa_obs
                where id = %s
                    and id not in (select id_taxa_obs from rubus.taxa_obs_ref_lookup where match_type is not null)
            """, (rec[0],))

        # Insert group member defined by reference list
        self.cur.execute("""
            SELECT rubus.insert_taxa_obs_group_member(%s, %s, %s, %s, %s)
        """, (short_group, taxa_obs_data[0][1], taxa_obs_data[0][2],  taxa_obs_data[0][3],  taxa_obs_data[0][4]))


        # Assert id_taxa_obs are related to group in taxa_obs_group_lookup
        query = f"""
            select *
            from rubus.taxa_obs_group_lookup_level_1_2_view glu
            join taxa_obs on glu.id_taxa_obs = taxa_obs.id
            where glu.short_group = '{short_group}'
            """
        result = pd.read_sql(query, self.conn)

        synonym_taxa_obs = [rec[1] for rec in taxa_obs_data]
        group_taxa_obs = [taxon for taxon in synonym_taxa_obs if taxon in result['scientific_name'].values]

        # Assert all observed names are in the group
        self.assertEqual(len(synonym_taxa_obs), len(group_taxa_obs))
