-- -----------------------------------------------------
-- Table `api.taxa_ref_sources
-- DESCRIPTION: This table contains the list of sources for taxa data with priority
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS api.taxa_ref_sources (
  source_id INTEGER PRIMARY KEY,
  source_name VARCHAR(255) NOT NULL,
  source_priority INTEGER NOT NULL
);

DELETE FROM api.taxa_ref_sources;

INSERT INTO api.taxa_ref_sources
VALUES (1002, 'CDPNQ', 1),
	(1001, 'Bryoquel', 2),
	(147, 'VASCAN', 3),
  	(11, 'GBIF Backbone Taxonomy', 4),
	(3, 'ITIS', 5),
	(1, 'Catalogue of Life', 6);

CREATE TABLE IF NOT EXISTS api.taxa_vernacular_sources(
	source_name VARCHAR(255) PRIMARY KEY,
	source_priority INTEGER NOT NULL
);

DELETE FROM api.taxa_vernacular_sources;

INSERT INTO api.taxa_vernacular_sources
VALUES ('CDPNQ', 1),
	('Eliso', 2),
	('Bryoquel', 3),
	('Database of Vascular Plants of Canada (VASCAN)', 4),
	('Integrated Taxonomic Information System (ITIS)', 5),
	('Checklist of Vermont Species', 6);


-- -----------------------------------------------------
-- DROP FUNCTION IF EXISTS api.__taxa_join_attributes(integer[]);
CREATE OR REPLACE FUNCTION api.__taxa_join_attributes(
	taxa_obs_id integer[])
    RETURNS TABLE(id_taxa_obs integer, observed_scientific_name text, valid_scientific_name text, rank text, sensitive boolean, vernacular_en text, vernacular_fr text, group_en text, group_fr text) 
    LANGUAGE 'sql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
SELECT
  id_taxa_obs,
  observed_scientific_name,
  valid_scientific_name,
  rank,
  sensitive,
  vernacular_en,
  vernacular_fr,
  group_en,
  group_fr
FROM api.taxa
WHERE id_taxa_obs = ANY($1)
$BODY$;

ALTER FUNCTION api.__taxa_join_attributes(integer[])
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION api.__taxa_join_attributes(integer[]) TO PUBLIC;

GRANT EXECUTE ON FUNCTION api.__taxa_join_attributes(integer[]) TO coleo;

GRANT EXECUTE ON FUNCTION api.__taxa_join_attributes(integer[]) TO read_only_all;

GRANT EXECUTE ON FUNCTION api.__taxa_join_attributes(integer[]) TO read_only_public;

GRANT EXECUTE ON FUNCTION api.__taxa_join_attributes(integer[]) TO read_write_all;

-- -----------------------------------------------------------------------------
-- VIEW api.taxa
-- DESCRIPTION List all observed taxons with their matched attributes from ref
--   ref sources and vernacular sources
-- -----------------------------------------------------------------------------

-- DROP VIEW if exists api.taxa CASCADE;
/*
	This selection creates a materialized view named 'api.taxa' that combines information from multiple tables to provide a comprehensive view of taxonomy data.
	
	The selection consists of several common table expressions (CTEs) that perform various data transformations and aggregations.
	
	CTEs:
	
	- all_ref: Retrieves information about taxa references for observed taxa, including scientific name, rank, source name, source priority, and source taxon key.
	- agg_ref: Aggregates the taxa references for each observed taxa into a JSON array.
	- best_ref: Selects the best taxa reference for each observed taxa based on source priority.
	- obs_group: Retrieves the group information for each observed taxa, including the English and French vernacular names of the group.
	- vernacular_all: Retrieves all vernacular names for observed taxa, including source priority, match type, and rank order.
	- best_vernacular: Selects the best vernacular names for each observed taxa based on source priority and language (English and French).
	- vernacular_group: Aggregates the vernacular names for each observed taxa into a JSON array.
	
	The final SELECT statement combines the information from the CTEs to generate the desired output, including observed scientific name, valid scientific name, rank, vernacular names (English and French), group names (English and French), vernacular names (aggregated), and taxa references (aggregated).
*/

-- DROP VIEW api.taxa_view;
CREATE OR REPLACE VIEW api.taxa_view
AS
SELECT
    taxa_obs.id AS id_taxa_obs,
    taxa_obs.scientific_name AS observed_scientific_name,
    ref_pref.scientific_name AS valid_scientific_name,
    ref_pref.rank,
    sensitive_group.id_group IS NOT NULL AS sensitive,
    vernacular_pref.vernacular_en,
    vernacular_pref.vernacular_fr,
    group_en.group_en,
    group_fr.group_fr,
    kingdom.scientific_name as kingdom,
    phylum.scientific_name as phylum,
    class.scientific_name as class,
    "order".scientific_name as "order",
    family.scientific_name as family,
    genus.scientific_name as genus,
    species.scientific_name as species
FROM taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred ref_pref ON taxa_obs.id = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_ref_vernacular_preferred vernacular_pref ON ref_pref.id_taxa_ref = vernacular_pref.id_taxa_ref
LEFT JOIN 
  (SELECT * FROM taxa_obs_group_lookup WHERE taxa_obs_group_lookup.short_group::text = 'SENSITIVE'::text)
    AS sensitive_group USING (id_taxa_obs)
LEFT JOIN 
  (SELECT DISTINCT ON (group_lu.id_taxa_obs) group_lu.id_taxa_obs, taxa_groups.vernacular_fr AS group_fr FROM taxa_obs_group_lookup group_lu 
    LEFT JOIN taxa_groups ON group_lu.id_group = taxa_groups.id WHERE taxa_groups.level = 1) AS group_fr USING (id_taxa_obs)
LEFT JOIN
  (SELECT DISTINCT ON (group_lu.id_taxa_obs) group_lu.id_taxa_obs, taxa_groups.vernacular_en AS group_en FROM taxa_obs_group_lookup group_lu
    LEFT JOIN taxa_groups ON group_lu.id_group = taxa_groups.id WHERE taxa_groups.level = 1) AS group_en USING (id_taxa_obs)
LEFT JOIN api.taxa_obs_ref_preferred kingdom ON kingdom.rank = 'kingdom' AND kingdom.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred phylum ON phylum.rank = 'phylum' AND phylum.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred class ON class.rank = 'class' AND class.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred "order" ON "order".rank = 'order' AND "order".id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred family ON family.rank = 'family' AND family.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred genus ON genus.rank = 'genus' AND genus.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN api.taxa_obs_ref_preferred species ON species.rank = 'species' AND species.id_taxa_obs = ref_pref.id_taxa_obs
WHERE ref_pref.is_match IS TRUE
  AND ref_pref.scientific_name IS NOT NULL;

-- DROP FUNCTION IF EXISTS api.refresh_taxa();
CREATE OR REPLACE FUNCTION api.refresh_taxa()
 RETURNS void
 LANGUAGE plpgsql
AS $BODY$
BEGIN
    DELETE FROM api.taxa;

	INSERT INTO api.taxa
	SELECT * FROM api.taxa_view;
END;
$BODY$;

-- DROP TABLE IF EXISTS api.taxa_table
CREATE TABLE IF NOT EXISTS api.taxa(
	id_taxa_obs integer NOT NULL,
	observed_scientific_name text NOT NULL,
	valid_scientific_name text NOT NULL,
	rank text NOT NULL,
	sensitive boolean NOT NULL,
	vernacular_en text,
	vernacular_fr text,
	group_en text,
	group_fr text,
	kingdom text,
	phylum text,
	class text,
	"order" text,
	family text,
	genus text,
	species text
)

ALTER TABLE IF EXISTS api.taxa
    OWNER TO coleo;

CREATE INDEX taxa_class_idx ON api.taxa (class);
CREATE INDEX taxa_family_idx ON api.taxa (family);
CREATE INDEX taxa_group_en_idx ON api.taxa (group_en);
CREATE INDEX taxa_group_fr_idx ON api.taxa (group_fr);
CREATE INDEX taxa_observed_scientific_name_idx ON api.taxa (observed_scientific_name);
CREATE INDEX taxa_order_idx ON api.taxa ("order");
CREATE INDEX taxa_phylum_idx ON api.taxa (phylum);
CREATE INDEX taxa_rank_idx ON api.taxa (rank);
CREATE INDEX taxa_species_idx ON api.taxa (species);
CREATE INDEX taxa_valid_scientific_name_idx ON api.taxa (valid_scientific_name);


-- DROP FUNCTION if exists api.match_taxa CASCADE;
CREATE OR REPLACE FUNCTION api.match_taxa (taxa_name TEXT)
RETURNS SETOF api.taxa
AS $$
select taxa.* from api.taxa, match_taxa_obs($1) taxa_obs
WHERE id_taxa_obs = taxa_obs.id
$$ LANGUAGE SQL STABLE;

ALTER FUNCTION api.match_taxa(text)
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION api.match_taxa(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION api.match_taxa(text) TO coleo;
GRANT EXECUTE ON FUNCTION api.match_taxa(text) TO read_only_all;
GRANT EXECUTE ON FUNCTION api.match_taxa(text) TO read_only_public;
GRANT EXECUTE ON FUNCTION api.match_taxa(text) TO read_write_all;


-- CREATE FUNCTION taxa_branch_tips that takes a list of id_taxa_obs values and
-- returns the number of unique taxa observed based on the tip-of-the-branch method

-- This function is used by the api.taxa_richness function to compute the number of
-- unique taxa observed based on the tip-of-the-branch method
DROP FUNCTION IF EXISTS api.taxa_branch_tips(integer[]);
CREATE OR REPLACE FUNCTION api.taxa_branch_tips (
    taxa_obs_ids integer[]
) RETURNS integer[] AS $$
	with nodes AS (
		select
			id_taxa_ref_valid,
			bool_and((coalesce(match_type = 'complex_closest_parent', false) or is_parent is true) is false) is_tip,
			min(id_taxa_obs) id_taxa_obs,
			count(id_taxa_ref_valid) count_taxa_ref
		from taxa_obs_ref_lookup obs_lookup
		WHERE obs_lookup.id_taxa_obs = any(taxa_obs_ids)
			and (match_type != 'complex' or match_type is null)
		group by id_taxa_ref_valid
	)
	select array_agg(distinct(id_taxa_obs)) id
	from nodes
	where is_tip is true
$$ LANGUAGE sql;

CREATE OR REPLACE AGGREGATE api.taxa_branch_tips (integer) (
	SFUNC = array_append,
	STYPE = integer[],
	FINALFUNC = api.taxa_branch_tips,
	INITCOND = '{}'
);

-- ---------------------------------------------------------------------------
-- CREATE function get_unique_species that takes a list of id_taxa_obs and
-- returns the number of unique taxa considering sub-species, varieties etc.
-- currently used in obs_summary
------------------------------------------------------------------------------
-- FUNCTION: api.get_unique_species(integer[])

-- DROP FUNCTION IF EXISTS api.get_unique_species(integer[]);

CREATE OR REPLACE FUNCTION api.get_unique_species(
	taxa_obs_ids integer[])
    RETURNS integer[]
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
    with nodes AS (
          select 
            bool_and((coalesce(match_type = 'complex_closest_parent', false) or is_parent is true) is false) is_tip, 
            min(id_taxa_obs) id_taxa_obs, 
            taxa_ref.scientific_name
          from taxa_obs_ref_lookup obs_lookup
            left join taxa_ref on obs_lookup.id_taxa_ref_valid = taxa_ref.id
          where obs_lookup.id_taxa_obs = any(taxa_obs_ids) 
            and (match_type != 'complex' or match_type is null) 
            and taxa_ref.rank = 'species' 
          group by id_taxa_obs, taxa_ref.id, scientific_name
        )
    SELECT array_agg(min_id_taxa_obs) AS unique_species_id
    FROM (
        SELECT min(id_taxa_obs) AS min_id_taxa_obs
        FROM nodes
        WHERE is_tip is true
        GROUP BY scientific_name
    )
    --    select array_agg(distinct(scientific_name)) unique_sp
    --    from nodes 
    --    where is_tip is true
$BODY$;
