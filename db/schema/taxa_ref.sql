-------------------------------------------------------------------------------
-- DESCRIPTION
-- Create table to contain taxa entities from sources and related ressources
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- CREATE FUNCTION rubus.taxa_match_sources
-- DESCRIPTION Uses python `bdqc_taxa` package to generate `taxa_ref` records
--  from taxonomic sources (ITIS, COL, etc) matched to input taxa name
-------------------------------------------------------------------------------
-- INSTALL python PL EXTENSION TO SUPPORT API CALL
CREATE EXTENSION IF NOT EXISTS plpython3u;

-- CREATE FUNCTION TO ACCESS REFERENCE TAXA FROM GLOBAL NAMES
DROP FUNCTION IF EXISTS rubus.match_taxa_sources(text, text, text);
CREATE OR REPLACE FUNCTION rubus.match_taxa_sources(
    name text,
    name_authorship text DEFAULT NULL,
    parent_scientific_name text DEFAULT NULL)
RETURNS TABLE (
    source_name text,
    source_id numeric,
    source_record_id text,
    scientific_name text,
    authorship text,
    rank text,
    rank_order integer,
    valid boolean,
    valid_srid text,
    classification_srids text[],
    match_type text,
    is_parent boolean
)
LANGUAGE plpython3u
AS $function$
from bdqc_taxa.taxa_ref import TaxaRef
import plpy
try:
  return TaxaRef.from_all_sources(name, name_authorship, parent_scientific_name)
except Exception as e:
  plpy.notice(f'Failed to match_taxa_sources: {name} {name_authorship}')
  raise Exception(e)
out = TaxaRef.from_all_sources(name, name_authorship)
return out
$function$;

-------------------------------------------------------------------------------
-- CREATE TABLE rubus.taxa_ref
-- DESCRIPTION Stores taxa attributes from reference sources
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rubus.taxa_ref (
    id SERIAL PRIMARY KEY,
    source_name text NOT NULL,
    source_id numeric,
    source_record_id text NOT NULL,
    scientific_name text NOT NULL,
    authorship text,
    rank text NOT NULL,
    valid boolean NOT NULL,
    valid_srid text NOT NULL,
    classification_srids text[],
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name, source_record_id)
);
CREATE INDEX IF NOT EXISTS source_id_srid_idx
  ON rubus.taxa_ref (source_id, valid_srid);

CREATE INDEX IF NOT EXISTS scientific_name_idx
  ON rubus.taxa_ref (scientific_name);


-------------------------------------------------------------------------------
-- CREATE public.taxa_obs to rubus.taxa_ref correspondance table
-------------------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS rubus.taxa_obs_ref_lookup (
        id_taxa_obs integer NOT NULL,
        id_taxa_ref integer NOT NULL,
        id_taxa_ref_valid integer NOT NULL,
        match_type text,
        is_parent boolean,
        UNIQUE (id_taxa_obs, id_taxa_ref)
    );

    CREATE INDEX IF NOT EXISTS id_taxa_obs_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_obs);

    CREATE INDEX IF NOT EXISTS id_taxa_ref_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_ref);

    CREATE INDEX IF NOT EXISTS id_taxa_ref_valid_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_ref_valid);

    -- Foreign key constraints

    -- ALTER TABLE rubus.taxa_obs_ref_lookup
    --     DROP CONSTRAINT IF EXISTS taxa_obs_ref_lookup_id_taxa_obs_fkey;

    ALTER TABLE rubus.taxa_obs_ref_lookup
        ADD CONSTRAINT taxa_obs_ref_lookup_id_taxa_obs_fkey
        FOREIGN KEY (id_taxa_obs)
        REFERENCES rubus.taxa_obs (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE;

    -- ALTER TABLE rubus.taxa_obs_ref_lookup
    --     DROP CONSTRAINT IF EXISTS taxa_obs_ref_lookup_id_taxa_ref_fkey;

    ALTER TABLE rubus.taxa_obs_ref_lookup
        ADD CONSTRAINT taxa_obs_ref_lookup_id_taxa_ref_fkey
        FOREIGN KEY (id_taxa_ref)
        REFERENCES rubus.taxa_ref (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE;

-- CREATE FUNCTIONS to update taxa_ref from taxa_obs records
-------------------------------------------------------------------------------

    -- DROP FUNCTION IF EXISTS insert_taxa_ref_from_taxa_obs(integer, text, text);
    CREATE OR REPLACE FUNCTION rubus.insert_taxa_ref_from_taxa_obs(
        taxa_obs_id integer,
        taxa_obs_scientific_name text,
        taxa_obs_authorship text DEFAULT NULL,
        taxa_obs_parent_scientific_name text DEFAULT NULL
    )
    RETURNS void AS
    $BODY$
    BEGIN
        DROP TABLE IF EXISTS temp_src_ref;
        CREATE TEMPORARY TABLE temp_src_ref AS (
            SELECT *
            FROM rubus.match_taxa_sources(taxa_obs_scientific_name, taxa_obs_authorship, taxa_obs_parent_scientific_name)
        );

        INSERT INTO rubus.taxa_ref (
            source_name,
            source_id,
            source_record_id,
            scientific_name,
            authorship,
            rank,
            valid,
            valid_srid,
            classification_srids
        )
        SELECT
            source_name,
            source_id,
            source_record_id,
            scientific_name,
            authorship,
            rank,
            valid,
            valid_srid,
            classification_srids
        FROM temp_src_ref
        ON CONFLICT DO NOTHING;

        INSERT INTO rubus.taxa_obs_ref_lookup (
                id_taxa_obs, id_taxa_ref, id_taxa_ref_valid, match_type, is_parent)
            SELECT
                taxa_obs_id AS id_taxa_obs,
                taxa_ref.id AS id_taxa_ref,
                valid_taxa_ref.id AS id_taxa_ref_valid,
             temp_src_ref.match_type AS match_type, 
             temp_src_ref.is_parent AS is_parent
            FROM
             temp_src_ref,
                rubus.taxa_ref,
                rubus.taxa_ref as valid_taxa_ref
            WHERE  
             temp_src_ref.source_id = taxa_ref.source_id
                AND temp_src_ref.source_record_id = taxa_ref.source_record_id
                and temp_src_ref.source_id = valid_taxa_ref.source_id
                and temp_src_ref.valid_srid = valid_taxa_ref.source_record_id
            ON CONFLICT DO NOTHING;
    END;
    $BODY$
    LANGUAGE 'plpgsql';

-------------------------------------------------------------------------------
-- REFRESH taxa_ref and taxa_obs_ref_lookup
-------------------------------------------------------------------------------

-- rubus.refresh_taxa_ref()
-- Completly delete and refresh all of taxa_ref and taxa_obs_ref_lookup
CREATE OR REPLACE FUNCTION rubus.refresh_taxa_ref(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    taxa_obs_record RECORD;
BEGIN
    DELETE FROM rubus.taxa_obs_ref_lookup;
    DELETE FROM rubus.taxa_ref;
    FOR taxa_obs_record IN SELECT * FROM public.taxa_obs LOOP
        BEGIN
            PERFORM rubus.insert_taxa_ref_from_taxa_obs(
            taxa_obs_record.id, taxa_obs_record.scientific_name, taxa_obs_record.authorship, taxa_obs_record.parent_scientific_name
            );
        EXCEPTION
            WHEN OTHERS THEN
            RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_obs_record.id, taxa_obs_record.scientific_name;
            CONTINUE;
        END;
    END LOOP;
    PERFORM rubus.taxa_ref_fix_synonyms();
    PERFORM rubus.fix_missing_source_parent();
END;
$BODY$;

ALTER FUNCTION rubus.refresh_taxa_ref()
    OWNER TO coleo;

-- Partially refresh taxa_ref, taxa_obs_ref_lookup,
-- taxa_vernacular and taxa_ref_vernacular_lookup
CREATE OR REPLACE FUNCTION rubus.refresh_taxa_partial()
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    subset_count INT;
    taxa_obs_record RECORD;
    taxa_ref_record RECORD;
BEGIN
    -- Create temporary table to only inject/refresh new data in taxa_obs
    CREATE TEMP TABLE IF NOT EXISTS subset_taxa_obs AS
        SELECT *
        FROM public.taxa_obs
        WHERE id NOT IN (SELECT id_taxa_obs FROM rubus.taxa_obs_ref_lookup);

    -- Count how many taxa are being processed
    SELECT COUNT(*) INTO subset_count FROM subset_taxa_obs;
    RAISE NOTICE 'Processing % rows from taxa_obs', subset_count;

    RAISE NOTICE 'Start processing taxa_ref';
    -- Refresh taxa_ref based on subset_taxa_obs
    FOR taxa_obs_record IN SELECT * FROM subset_taxa_obs LOOP
        BEGIN
            PERFORM rubus.insert_taxa_ref_from_taxa_obs(
            taxa_obs_record.id, taxa_obs_record.scientific_name, taxa_obs_record.authorship, taxa_obs_record.parent_scientific_name
            );
        EXCEPTION
            WHEN OTHERS THEN
            RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_obs_record.id, taxa_obs_record.scientific_name;
            CONTINUE;
        END;
    END LOOP;
    
    -- Perform function fix_synonyms
    PERFORM rubus.taxa_ref_fix_synonyms();
    PERFORM rubus.fix_missing_source_parent();

    RAISE NOTICE 'Start processing taxa_vernacular';

    FOR taxa_ref_record IN 
        SELECT
          array_agg(taxa_ref.id)::integer[] AS id_taxa_ref,
          taxa_ref.scientific_name,
          taxa_ref.rank
        FROM subset_taxa_obs
        JOIN rubus.taxa_obs_ref_lookup ref_lu ON subset_taxa_obs.id = ref_lu.id_taxa_obs
        JOIN rubus.taxa_ref ON ref_lu.id_taxa_ref = taxa_ref.id
        GROUP BY taxa_ref.scientific_name, taxa_ref.rank
    LOOP
        BEGIN
            PERFORM rubus.insert_taxa_vernacular_from_taxa_ref(taxa_ref_record.id_taxa_ref, taxa_ref_record.scientific_name, taxa_ref_record.rank);
        EXCEPTION
            WHEN OTHERS THEN
            RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_ref_record.id, taxa_ref_record.scientific_name;
            CONTINUE;
        END;
    END LOOP;

    -- Perform function fix_caribou
    PERFORM rubus.taxa_vernacular_fix_caribou();

    -- Drop temporary table
    DROP TABLE IF EXISTS subset_taxa_obs;

END;
$BODY$;

ALTER FUNCTION rubus.refresh_taxa_partial()
    OWNER TO coleo;

-- DROP FUNCTION IF EXISTS rubus.taxa_ref_fix_synonyms();
CREATE OR REPLACE FUNCTION rubus.taxa_ref_fix_synonyms(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
DROP TABLE IF EXISTS taxa_obs_ref_cdpnq_synonym_fix_lookup;
CREATE TEMPORARY TABLE taxa_obs_ref_cdpnq_synonym_fix_lookup AS (
SELECT
    distinct on (cdpnq_lu.id_taxa_ref, synonym_obs_lu.id_taxa_obs)
    cdpnq_lu.id_taxa_ref,
    cdpnq_lu.id_taxa_ref AS id_taxa_ref_valid,
    synonym_obs_lu.id_taxa_obs,
    synonym_obs_lu.match_type,
    synonym_obs_lu.is_parent
FROM rubus.taxa_ref cdpnq_ref
JOIN rubus.taxa_obs_ref_lookup cdpnq_lu ON cdpnq_ref.id = cdpnq_lu.id_taxa_ref
JOIN rubus.taxa_obs_ref_lookup gbif_lu ON cdpnq_lu.id_taxa_obs = gbif_lu.id_taxa_obs
-- taxa_ref join to filter only GBIF Backbone Taxonomy sources
JOIN rubus.taxa_ref gbif_ref ON gbif_lu.id_taxa_ref = gbif_ref.id
    AND gbif_ref.source_name = 'GBIF Backbone Taxonomy'
    AND cdpnq_ref.rank = gbif_ref.rank
JOIN rubus.taxa_obs_ref_lookup synonym_obs_lu ON gbif_ref.id = synonym_obs_lu.id_taxa_ref
WHERE cdpnq_ref.source_name = 'CDPNQ'
    AND cdpnq_ref.valid IS TRUE
    -- filter out records already in taxa_obs_ref_lookup
    AND (synonym_obs_lu.id_taxa_obs, cdpnq_lu.id_taxa_ref) NOT IN (
        SELECT id_taxa_obs, id_taxa_ref
        FROM rubus.taxa_obs_ref_lookup
    )
);

DELETE FROM rubus.taxa_obs_ref_lookup
WHERE (id_taxa_obs, id_taxa_ref) IN (
    SELECT id_taxa_obs, id_taxa_ref
    FROM taxa_obs_ref_cdpnq_synonym_fix_lookup
);

INSERT INTO rubus.taxa_obs_ref_lookup (id_taxa_obs, id_taxa_ref, id_taxa_ref_valid, match_type, is_parent)
SELECT
    id_taxa_obs,
    id_taxa_ref,
    id_taxa_ref_valid,
    match_type,
    is_parent
FROM taxa_obs_ref_cdpnq_synonym_fix_lookup
ON CONFLICT DO NOTHING;

END;
$BODY$;

ALTER FUNCTION rubus.taxa_ref_fix_synonyms()
    OWNER TO coleo;


CREATE OR REPLACE FUNCTION rubus.fix_missing_source_parent()
RETURNS void
LANGUAGE 'plpgsql'

AS $BODY$
BEGIN

INSERT INTO rubus.taxa_obs_ref_lookup (id_taxa_obs, id_taxa_ref, id_taxa_ref_valid, match_type, is_parent)
SELECT DISTINCT ON (cur_parent_lu.id_taxa_obs, new_parent_ref.id)
    cur_parent_lu.id_taxa_obs,
    new_parent_ref.id as id_taxa_ref,
    new_parent_ref.id as id_taxa_ref_valid,
    cur_parent_lu.match_type,
    cur_parent_lu.is_parent
FROM rubus.taxa_obs_ref_lookup cur_parent_lu
JOIN rubus.taxa_ref cur_parent_ref ON cur_parent_lu.id_taxa_ref = cur_parent_ref.id
JOIN rubus.taxa_ref new_parent_ref USING (scientific_name, rank)
WHERE (cur_parent_lu.id_taxa_obs, new_parent_ref.id) NOT IN (SELECT id_taxa_obs, id_taxa_ref FROM rubus.taxa_obs_ref_lookup)
    AND cur_parent_lu.is_parent IS TRUE
    AND new_parent_ref.valid IS TRUE
    ON CONFLICT DO NOTHING;
END;
$BODY$;

ALTER FUNCTION rubus.fix_missing_source_parent()
    OWNER TO coleo;