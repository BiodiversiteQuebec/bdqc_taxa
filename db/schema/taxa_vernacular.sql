-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- % TABLE taxa_vernacular
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- DROP TABLE IF EXISTS public.taxa_vernacular CASCADE;
CREATE TABLE IF NOT EXISTS public.taxa_vernacular
(
    id integer NOT NULL DEFAULT nextval('taxa_vernacular_id_seq'::regclass),
    source_name text  NOT NULL,
    source_record_id text NOT NULL,
    name text  NOT NULL,
    language text  NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by text NOT NULL DEFAULT CURRENT_USER,
    rank text,
    rank_order integer NOT NULL DEFAULT '-1'::integer,
    preferred boolean NOT NULL DEFAULT FALSE,
    CONSTRAINT taxa_vernacular_pkey PRIMARY KEY (id),
    CONSTRAINT taxa_vernacular_source_name_source_record_id_name_language_key UNIQUE (source_name, source_record_id, name, language)
)

ALTER TABLE IF EXISTS public.taxa_vernacular
    OWNER to coleo;

CREATE INDEX IF NOT EXISTS taxa_vernacular_source_name_idx
  ON public.taxa_vernacular (source_name);

CREATE INDEX IF NOT EXISTS taxa_vernacular_source_record_id_idx
    ON public.taxa_vernacular (source_record_id);

CREATE INDEX IF NOT EXISTS taxa_vernacular_language_idx
    ON public.taxa_vernacular (language);

CREATE INDEX IF NOT EXISTS taxa_vernacular_name_idx
    ON public.taxa_vernacular (name);

CREATE INDEX IF NOT EXISTS taxa_vernacular_rank_order_idx
    ON public.taxa_vernacular (rank_order);

-- Trigger: update_modified_at
-- DROP TRIGGER IF EXISTS update_modified_at ON public.taxa_vernacular;
CREATE OR REPLACE TRIGGER update_modified_at
    BEFORE UPDATE 
    ON public.taxa_vernacular
    FOR EACH ROW
    EXECUTE FUNCTION public.trigger_set_timestamp();


-- DROP TRIGGER IF EXISTS update_modified_at ON public.taxa_vernacular;
-- CREATE TRIGGER update_modified_at
--   BEFORE UPDATE ON public.taxa_vernacular FOR EACH ROW
--   EXECUTE PROCEDURE trigger_set_timestamp();

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- % TABLE taxa_vernacular_ref_lookup
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- DROP TABLE IF EXISTS public.taxa_ref_vernacular_lookup CASCADE;
CREATE TABLE IF NOT EXISTS public.taxa_ref_vernacular_lookup
(
    id_taxa_ref integer NOT NULL,
    id_taxa_vernacular integer NOT NULL,
    CONSTRAINT taxa_ref_vernacular_lookup_id_taxa_ref_id_taxa_vernacular_key UNIQUE (id_taxa_ref, id_taxa_vernacular)
);

ALTER TABLE IF EXISTS public.taxa_ref_vernacular_lookup 
    OWNER to coleo;

CREATE INDEX taxa_ref_vernacular_lookup_id_taxa_ref_idx ON public.taxa_ref_vernacular_lookup (id_taxa_ref);
CREATE INDEX taxa_ref_vernacular_lookup_id_taxa_vernacular_idx ON public.taxa_ref_vernacular_lookup (id_taxa_vernacular);

  
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- % FUNCTION taxa_vernacular_from_names
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

--DROP FUNCTION IF EXISTS public.taxa_vernacular_from_match(text, text);
CREATE OR REPLACE FUNCTION public.taxa_vernacular_from_match(
	scientific_name text,
    rank text DEFAULT NULL)
    RETURNS TABLE(source text, source_taxon_key text, name text, language text, rank text, rank_order integer, preferred boolean)
    LANGUAGE 'plpython3u'
AS $BODY$
from bdqc_taxa.vernacular import Vernacular
out = Vernacular.from_match(scientific_name, rank)
return out
$BODY$;

ALTER FUNCTION public.taxa_vernacular_from_match(text, text)
    OWNER TO coleo;

-- TEST
-- explain analyze select * from public.taxa_vernacular_from_match('Picoides villosus');

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- % FUNCTION insert_taxa_vernacular_from_taxa_ref
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
--DROP FUNCTION IF EXISTS public.insert_taxa_vernacular_from_taxa_ref(integer, text, text);
CREATE OR REPLACE FUNCTION public.insert_taxa_vernacular_from_taxa_ref(
    id_taxa_ref integer[],
	scientific_name text,
    rank text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    DROP TABLE IF EXISTS temp_src_vernacular;
    CREATE TEMPORARY TABLE temp_src_vernacular AS (
        SELECT *
        FROM public.taxa_vernacular_from_match($2, $3)
    );
    RAISE NOTICE 'Inserting (%, %)', $2, $3;

    INSERT INTO public.taxa_vernacular (
        source_name,
        source_record_id,
        name,
        language,
        "rank",
        rank_order,
        preferred
    )
    SELECT 
        temp_src_vernacular.source,
        temp_src_vernacular.source_taxon_key,
        temp_src_vernacular.name,
        temp_src_vernacular.language,
        temp_src_vernacular.rank,
        temp_src_vernacular.rank_order,
        temp_src_vernacular.preferred
    FROM temp_src_vernacular
    ON CONFLICT DO NOTHING;

    INSERT INTO public.taxa_ref_vernacular_lookup (
            id_taxa_ref, id_taxa_vernacular
    )
    SELECT 
        ref_id,
        v.id AS id_taxa_vernacular
    FROM unnest($1) AS ref_id
    CROSS JOIN (
        SELECT taxa_vernacular.id
        FROM temp_src_vernacular
        JOIN taxa_vernacular
          ON temp_src_vernacular.source = taxa_vernacular.source_name
         AND temp_src_vernacular.source_taxon_key = taxa_vernacular.source_record_id
    ) AS v
    ON CONFLICT DO NOTHING;
END;
$BODY$;

ALTER FUNCTION public.insert_taxa_vernacular_from_taxa_ref(integer, text, text)
    OWNER TO coleo;

------------------------------------------------------------------------
-- FUNCTION refresh_taxa_vernacular
------------------------------------------------------------------------

DROP FUNCTION IF EXISTS public.taxa_vernacular_fix_caribou();
CREATE OR REPLACE FUNCTION public.taxa_vernacular_fix_caribou(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DELETE FROM taxa_ref_vernacular_lookup vlu
USING taxa_ref, taxa_obs_ref_lookup, taxa_vernacular, taxa_obs
WHERE taxa_ref.scientific_name NOT ilike 'Rangifer%'
	AND taxa_obs.scientific_name NOT ilike 'Rangifer%'
	AND taxa_ref.id = taxa_obs_ref_lookup.id_taxa_ref
	AND taxa_obs.id = taxa_obs_ref_lookup.id_taxa_obs
	AND taxa_vernacular.name ilike 'caribou'
	AND taxa_vernacular.id = vlu.id_taxa_vernacular
	AND vlu.id_taxa_ref = taxa_obs_ref_lookup.id_taxa_ref
    AND taxa_obs_ref_lookup.id_taxa_obs = taxa_obs.id;
$BODY$;

-- Function refresh_taxa_vernacular()
-- DROP FUNCTION IF EXISTS public.refresh_taxa_vernacular();
CREATE OR REPLACE FUNCTION public.refresh_taxa_vernacular(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    taxa_ref_record RECORD;
BEGIN
    DELETE FROM taxa_ref_vernacular_lookup;
    DELETE FROM taxa_vernacular;
    FOR taxa_ref_record IN 
        SELECT
          array_agg(id)::integer[] AS id_taxa_ref,
          scientific_name,
          rank
        FROM taxa_ref
        GROUP BY scientific_name, rank
        ORDER BY scientific_name
    LOOP
        BEGIN
            PERFORM public.insert_taxa_vernacular_from_taxa_ref(taxa_ref_record.id_taxa_ref, taxa_ref_record.scientific_name, taxa_ref_record.rank);
        EXCEPTION
            WHEN OTHERS THEN
            RAISE NOTICE 'Error inserting record with id % and scientific name %', taxa_ref_record.id_taxa_ref, taxa_ref_record.scientific_name;
            CONTINUE;
        END;
    END LOOP;
    PERFORM taxa_vernacular_fix_caribou();
END;
$BODY$;

ALTER FUNCTION public.refresh_taxa_vernacular()
    OWNER TO coleo;

