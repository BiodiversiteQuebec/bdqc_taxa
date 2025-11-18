-- DROP TABLE IF EXISTS rubus.taxa_groups;
CREATE TABLE IF NOT EXISTS rubus.taxa_groups (
    id serial primary key,
    short varchar(20),
    vernacular_fr text,
    vernacular_en text,
    level integer,
    source_desc text,
    groups_within text[]
);

ALTER TABLE IF EXISTS rubus.taxa_groups
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_groups FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_groups FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_groups TO coleo;
GRANT SELECT ON TABLE rubus.taxa_groups TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_groups TO read_write_all;

CREATE INDEX IF NOT EXISTS taxa_groups_short_idx ON rubus.taxa_groups (short);
CREATE UNIQUE INDEX IF NOT EXISTS taxa_groups_short_unique_idx ON rubus.taxa_groups (short);

COMMENT ON TABLE rubus.taxa_groups IS 'Table of taxa groups definitions';
--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DESCRIPTION OF LEVELS
-- 0: All quebec taxa, members are gathered from the observations within_quebec
-- 1: High level groups, Contains exclusive taxas to other level 1 groups
-- 2: Application level groups defined by scientific_name, From specific list for specific analysis, may overlaps with other groups
-- 3: Application level groups defined by other groups instead of scientific_name

INSERT INTO rubus.taxa_groups (short, id, vernacular_fr, vernacular_en, level, source_desc) VALUES
('AMPHIBIANS', 1, 'Amphibiens', 'Amphibians', 1, NULL),
('BIRDS', 2, 'Oiseaux', 'Birds', 1, NULL),
('MAMMALS', 3, 'Mammifères', 'Mammals', 1, NULL),
('REPTILES', 4, 'Reptiles', 'Reptiles', 1, NULL),
('FISH', 5, 'Poissons', 'Fish', 1, NULL),
('TUNICATES', 6, 'Tuniciers', 'Tunicates', 1, NULL),
('LANCELETS', 7, 'Céphalocordés', 'Lancelets', 1, NULL),
('ARTHROPODS', 8, 'Arthropodes', 'Arthropods', 1, NULL),
('OTHER_INVERTEBRATES', 9, 'Autres invertébrés', 'Other invertebrates', 1, NULL),
('OTHER_TAXONS', 10, 'Autres taxons', 'Other taxons', 1, NULL),
('FUNGI', 11, 'Mycètes', 'Fungi', 1, NULL),
('ANGIOSPERMS', 12, 'Angiospermes', 'Angiosperms', 1, NULL),
('CONIFERS', 13, 'Conifères', 'Conifers', 1, NULL),
('VASCULAR_CRYPTOGAM', 14, 'Cryptogames vasculaires', 'Vascular cryptogam', 1, NULL),
('OTHER_GYMNOSPERMS', 15, 'Autres gymnospermes', 'Other gymnosperms', 1, NULL),
('ALGAE', 16, 'Algues', 'Algae', 1, NULL),
('BRYOPHYTES', 17, 'Bryophytes', 'Bryophytes', 1, NULL),
('OTHER_PLANTS', 18, 'Autres plantes', 'Other plants', 1, NULL),
('ALL_SPECIES', 19, 'Toutes les espèces', 'All species', 0, NULL),
('INVASIVE_SPECIES', 25, 'Espèce envahissante', 'Invasive species', 2, 'Sentinelle, Agriculture, environnement et ressources naturelles Québec'),
('SENTINELLE_INVASIVE', 33, 'Espèce exotique envahissante', 'Exotic invasive species', 2, 'SENTINELLE'),
('PRINCIPAL_INVASIVE', 34, 'Principales espèces exotiques envahissantes', NULL, 2, 'Agriculture, environnement et ressources naturelles Québec'),
('CDPNQ_SUSC', 21, 'Espèce susceptible', NULL, 2, 'CDPNQ'),
('CDPNQ_VUL', 22, 'Espèce vulnérable', NULL, 2, 'CDPNQ'),
('CDPNQ_VUL_HARVEST', 23, 'Espèce vulnérable à la récolte', NULL, 2, 'CDPNQ'),
('CDPNQ_ENDANGERED', 24, 'Espèce menacée', NULL, 2, 'CDPNQ'),
('CDPNQ_S1', 27, 'Rang S1', 'S1 Rank', 2, 'CDPNQ'),
('CDPNQ_S2', 28, 'Rang S2', 'S2 Rank', 2, 'CDPNQ'),
('CDPNQ_S3', 29, 'Rang S3', 'S3 Rank', 2, 'CDPNQ'),
('SENSITIVE', 31, 'Espèce sensibles', 'Sensitive species', 2, 'CDPNQ'),
('CDPNQ_EMV', 32, 'Espèces menacées, vulnérables ou susceptibles', 'At-risk species', 2, 'CDPNQ');

INSERT INTO rubus.taxa_groups (short, id, vernacular_fr, vernacular_en, level, groups_within)
VALUES
    -- ('CDPNQ_RISK', ARRAY['CDPNQ_S1', 'CDPNQ_S2', 'CDPNQ_S3']),
    -- ('CDPNQ_STATUS', ARRAY['CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST', 'CDPNQ_ENDANGERED']);
    ('CDPNQ_RISK', 30, 'En situation précaire', 'At risk', 3, ARRAY['CDPNQ_S1', 'CDPNQ_S2', 'CDPNQ_S3']),
    ('CDPNQ_STATUS', 26, 'Espèces à statut CDPNQ', 'Species at risk ', 3, ARRAY['CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST', 'CDPNQ_ENDANGERED']);

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_group_members CASCADE;
CREATE TABLE rubus.taxa_group_members (
    short varchar(20),
    id_taxa_obs NOT NULL
    CONSTRAINT taxa_group_members_id_taxa_obs_fkey FOREIGN KEY (id_taxa_obs)
        REFERENCES public.taxa_obs (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

ALTER TABLE IF EXISTS rubus.taxa_group_members
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_group_members FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_group_members FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_group_members TO coleo;
GRANT SELECT ON TABLE rubus.taxa_group_members TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_group_members TO read_write_all;

CREATE INDEX IF NOT EXISTS taxa_group_members_id_taxa_obs_idx
    ON rubus.taxa_group_members (id_taxa_obs);

CREATE UNIQUE INDEX IF NOT EXISTS idx_taxa_group_members_short_id_taxa_obs
    ON rubus.taxa_group_members (short, id_taxa_obs);

COMMENT ON TABLE rubus.taxa_group_members IS 'Table linking taxa_obs to taxa_groups';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP FUNCTION IF EXISTS rubus.insert_taxa_obs_group_member(character, text, text, text, text);
CREATE OR REPLACE FUNCTION rubus.insert_taxa_obs_group_member(
	short_group character,
	scientific_name text,
	authorship text DEFAULT ''::text,
	rank text DEFAULT ''::text,
	parent_scientific_name text DEFAULT ''::text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
 
DECLARE
    taxa_obs_id integer;
BEGIN
    -- 3.1. Insérer dans taxa_obs si non existant
    INSERT INTO public.taxa_obs (scientific_name, authorship, rank, parent_scientific_name)
    VALUES ($2, $3, $4, $5)
    ON CONFLICT DO NOTHING;

    -- 3.2. Récupérer l'id du taxa_obs
    SELECT id INTO taxa_obs_id FROM public.taxa_obs t
      WHERE t.scientific_name = $2
        AND t.authorship = $3
        AND t.rank = $4
        AND t.parent_scientific_name = $5
      LIMIT 1;

    -- 3.3. Insérer dans taxa_group_members avec id_taxa_obs
    INSERT INTO rubus.taxa_group_members (short, id_taxa_obs)
    VALUES ($1, taxa_obs_id)
    ON CONFLICT DO NOTHING;

    -- 3.4. Rafraîchir taxa_ref pour ce taxa_obs
    BEGIN
        PERFORM rubus.insert_taxa_ref_from_taxa_obs(taxa_obs_id, $2, $3, $5);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_obs_id, $2;
    END;
END;
$BODY$;

ALTER FUNCTION rubus.insert_taxa_obs_group_member(character, text, text, text, text)
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION rubus.insert_taxa_obs_group_member(character, text, text, text, text) TO coleo;
GRANT EXECUTE ON FUNCTION rubus.insert_taxa_obs_group_member(character, text, text, text, text) TO read_write_all;
REVOKE ALL ON FUNCTION rubus.insert_taxa_obs_group_member(character, text, text, text, text) FROM PUBLIC;

COMMENT ON FUNCTION rubus.insert_taxa_obs_group_member(character, text, text, text, text) IS 'Function to insert a taxa_obs and link it to a taxa_group based on scientific name and group short name';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DELETE FROM rubus.taxa_group_members;
-- Refactored to use SELECT instead of COPY
WITH taxa_inserts(short, scientific_name) AS (
    VALUES
        ('AMPHIBIANS', 'Amphibia'),
        ('BIRDS', 'Aves'),
        ('MAMMALS', 'Mammalia'),
        ('REPTILES', 'Reptilia'),
        ('FISH', 'Myxini'),
        ('FISH', 'Holocephali'),
        ('FISH', 'Actinopterygii'),
        ('FISH', 'Cephalaspidomorphi'),
        ('FISH', 'Elasmobranchii'),
        ('FISH', 'Coelacanthiformes'),
        ('TUNICATES', 'Ascidiacea'),
        ('TUNICATES', 'Thaliacea'),
        ('TUNICATES', 'Appendicularia'),
        ('LANCELETS', 'Leptocardii'),
        ('ARTHROPODS', 'Arthropoda'),
        ('OTHER_INVERTEBRATES', 'Hemichordata'),
        ('OTHER_INVERTEBRATES', 'Micrognathozoa'),
        ('OTHER_INVERTEBRATES', 'Mollusca'),
        ('OTHER_INVERTEBRATES', 'Myxozoa'),
        ('OTHER_INVERTEBRATES', 'Nematoda'),
        ('OTHER_INVERTEBRATES', 'Nematomorpha'),
        ('OTHER_INVERTEBRATES', 'Nemertea'),
        ('OTHER_INVERTEBRATES', 'Onychophora'),
        ('OTHER_INVERTEBRATES', 'Orthonectida'),
        ('OTHER_INVERTEBRATES', 'Phoronida'),
        ('OTHER_INVERTEBRATES', 'Placozoa'),
        ('OTHER_INVERTEBRATES', 'Platyhelminthes'),
        ('OTHER_INVERTEBRATES', 'Porifera'),
        ('OTHER_INVERTEBRATES', 'Rotifera'),
        ('OTHER_INVERTEBRATES', 'Sipuncula'),
        ('OTHER_INVERTEBRATES', 'Xenacoelomorpha'),
        ('OTHER_INVERTEBRATES', 'Tardigrada'),
        ('OTHER_INVERTEBRATES', 'Acanthocephala'),
        ('OTHER_INVERTEBRATES', 'Annelida'),
        ('OTHER_INVERTEBRATES', 'Brachiopoda'),
        ('OTHER_INVERTEBRATES', 'Bryozoa'),
        ('OTHER_INVERTEBRATES', 'Cephalorhyncha'),
        ('OTHER_INVERTEBRATES', 'Chaetognatha'),
        ('OTHER_INVERTEBRATES', 'Cnidaria'),
        ('OTHER_INVERTEBRATES', 'Ctenophora'),
        ('OTHER_INVERTEBRATES', 'Cycliophora'),
        ('OTHER_INVERTEBRATES', 'Dicyemida'),
        ('OTHER_INVERTEBRATES', 'Echinodermata'),
        ('OTHER_INVERTEBRATES', 'Entoprocta'),
        ('OTHER_INVERTEBRATES', 'Gastrotricha'),
        ('OTHER_INVERTEBRATES', 'Gnathostomulida'),
        ('OTHER_TAXONS', 'Protozoa'),
        ('OTHER_TAXONS', 'Viruses'),
        ('OTHER_TAXONS', 'Chromista'),
        ('OTHER_TAXONS', 'Bacteria'),
        ('OTHER_TAXONS', 'Archaea'),
        ('FUNGI', 'Fungi'),
        ('ANGIOSPERMS', 'Magnoliopsida'),
        ('ANGIOSPERMS', 'Liliopsida'),
        ('CONIFERS', 'Pinopsida'),
        ('VASCULAR_CRYPTOGAM', 'Lycopodiopsida'),
        ('VASCULAR_CRYPTOGAM', 'Polypodiopsida'),
        ('OTHER_GYMNOSPERMS', 'Gnetopsida'),
        ('OTHER_GYMNOSPERMS', 'Cycadopsida'),
        ('OTHER_GYMNOSPERMS', 'Ginkgoopsida'),
        ('ALGAE', 'Chlorophyta'),
        ('ALGAE', 'Charophyta'),
        ('ALGAE', 'Rhodophyta'),
        ('BRYOPHYTES', 'Bryophyta'),
        ('OTHER_PLANTS', 'Glaucophyta'),
        ('OTHER_PLANTS', 'Anthocerotophyta'),
        ('OTHER_PLANTS', 'Marchantiophyta')
)
SELECT rubus.insert_taxa_obs_group_member(short, scientific_name)
FROM taxa_inserts;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_obs_group_lookup;
CREATE TABLE IF NOT EXISTS rubus.taxa_obs_group_lookup
(
    id_taxa_obs integer NOT NULL,
    id_group integer NOT NULL,
    short_group text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT taxa_obs_group_lookup_id_group_fkey FOREIGN KEY (id_group)
        REFERENCES rubus.taxa_groups (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT taxa_obs_group_lookup_id_taxa_obs_fkey FOREIGN KEY (id_taxa_obs)
        REFERENCES public.taxa_obs (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT taxa_obs_group_lookup_short_group_fkey FOREIGN KEY (short_group)
        REFERENCES rubus.taxa_groups (short) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

ALTER TABLE IF EXISTS rubus.taxa_obs_group_lookup
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_obs_group_lookup FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_obs_group_lookup FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_obs_group_lookup TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_group_lookup TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_obs_group_lookup TO read_write_all;

CREATE UNIQUE INDEX idx_taxa_obs_group_lookup ON rubus.taxa_obs_group_lookup (id_taxa_obs, id_group, short_group);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_id_taxa_obs_idx
  ON rubus.taxa_obs_group_lookup (id_taxa_obs);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_id_group_idx
    ON rubus.taxa_obs_group_lookup (id_group);

CREATE INDEX IF NOT EXISTS taxa_obs_group_lookup_short_group_idx
    ON rubus.taxa_obs_group_lookup (short_group);

COMMENT ON TABLE rubus.taxa_obs_group_lookup IS 'Lookup table linking taxa_obs to taxa_groups';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP VIEW rubus.taxa_obs_group_lookup_level_1_2_view;
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_level_1_2_view AS
SELECT DISTINCT
    obs_lookup.id_taxa_obs, 
    taxa_groups.id AS id_group, 
    taxa_groups.short AS short_group
FROM rubus.taxa_group_members group_m
JOIN rubus.taxa_groups ON taxa_groups.short = group_m.short
JOIN rubus.taxa_obs_ref_lookup match_lu
    ON group_m.id_taxa_obs = match_lu.id_taxa_obs
    AND match_lu.is_parent IS FALSE
LEFT JOIN rubus.taxa_obs_ref_lookup obs_lookup
    ON match_lu.id_taxa_ref_valid = obs_lookup.id_taxa_ref
WHERE taxa_groups.level = ANY(ARRAY[1, 2]);

ALTER TABLE rubus.taxa_obs_group_lookup_level_1_2_view
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_obs_group_lookup_level_1_2_view TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_group_lookup_level_1_2_view TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_obs_group_lookup_level_1_2_view TO read_write_all;

COMMENT ON VIEW rubus.taxa_obs_group_lookup_level_1_2_view IS 'View to link taxa_obs to taxa_groups of level 1 and 2';
--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP VIEW rubus.taxa_obs_group_lookup_quebec_view;
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_quebec_view AS
SELECT DISTINCT ON (id_taxa_obs)
    id_taxa_obs, 
    taxa_groups.id AS id_group, 
    taxa_groups.short AS short_group
FROM observations_partitions.within_quebec, taxa_groups
WHERE level = 0;

ALTER TABLE rubus.taxa_obs_group_lookup_quebec_view
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_obs_group_lookup_quebec_view TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_group_lookup_quebec_view TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_obs_group_lookup_quebec_view TO read_write_all;

COMMENT ON VIEW rubus.taxa_obs_group_lookup_quebec_view IS 'View to link taxa_obs to taxa_groups of level 0 (within Quebec)';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP VIEW rubus.taxa_obs_group_lookup_level_3_view;
CREATE OR REPLACE VIEW rubus.taxa_obs_group_lookup_level_3_view AS
SELECT 
    level_1_2.id_taxa_obs, 
    level_3_groups.id AS id_group, 
    level_3_groups.short AS short_group
FROM rubus.taxa_groups AS level_3_groups
JOIN rubus.taxa_obs_group_lookup_level_1_2_view AS level_1_2
    ON level_1_2.short_group = ANY(level_3_groups.groups_within)
WHERE level_3_groups.level = 3;

ALTER TABLE rubus.taxa_obs_group_lookup_level_3_view
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_obs_group_lookup_level_3_view TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_group_lookup_level_3_view TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_obs_group_lookup_level_3_view TO read_write_all;

COMMENT ON VIEW rubus.taxa_obs_group_lookup_level_3_view IS 'View to link taxa_obs to taxa_groups of level 3 based';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP FUNCTION IF EXISTS rubus.refresh_taxa_obs_group_lookup();
CREATE OR REPLACE FUNCTION rubus.refresh_taxa_obs_group_lookup(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    DELETE FROM rubus.taxa_obs_group_lookup;

    INSERT INTO rubus.taxa_obs_group_lookup (id_taxa_obs, id_group, short_group)
    SELECT * FROM rubus.taxa_obs_group_lookup_level_1_2_view
    UNION
    SELECT * FROM rubus.taxa_obs_group_lookup_quebec_view
    UNION
    SELECT * FROM rubus.taxa_obs_group_lookup_level_3_view
    ON CONFLICT DO NOTHING;
END;
$BODY$;

ALTER FUNCTION rubus.refresh_taxa_obs_group_lookup()
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION rubus.refresh_taxa_obs_group_lookup() TO coleo;
GRANT EXECUTE ON FUNCTION rubus.refresh_taxa_obs_group_lookup() TO read_write_all;
REVOKE ALL ON FUNCTION rubus.refresh_taxa_obs_group_lookup() FROM PUBLIC;

COMMENT ON FUNCTION rubus.refresh_taxa_obs_group_lookup() IS 'Function to refresh the taxa_obs_group_lookup table from the various views';