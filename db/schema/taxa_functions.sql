-- DROP FUNCTION IF EXISTS api.__taxa_join_attributes(integer[]);
CREATE OR REPLACE FUNCTION rubus.__taxa_join_attributes(
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

ALTER FUNCTION rubus.__taxa_join_attributes(integer[])
    OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP FUNCTION IF EXISTS api.match_taxa(text);
CREATE OR REPLACE FUNCTION api.match_taxa(
	taxa_name text)
    RETURNS SETOF api.taxa 
    LANGUAGE 'sql'
    COST 100
    STABLE PARALLEL SAFE
    ROWS 1000

AS $BODY$
WITH match_pref AS (
    SELECT ref_pref.scientific_name
    FROM rubus.taxa_ref AS matched_ref
        JOIN rubus.taxa_obs_ref_lookup ref_lu ON matched_ref.id = ref_lu.id_taxa_ref
        JOIN rubus.taxa_obs_ref_preferred ref_pref using (id_taxa_obs)
        JOIN rubus.taxa_ref_sources src ON matched_ref.source_name = src.source_name
    WHERE matched_ref.scientific_name ILIKE $1
        AND ref_lu.match_type IS DISTINCT FROM 'complex'
        AND is_match IS TRUE
        AND (ref_lu.id_taxa_obs, src.source_priority) IN (
          SELECT ref_lu2.id_taxa_obs, min(src2.source_priority)
          FROM rubus.taxa_obs_ref_lookup ref_lu2
          JOIN rubus.taxa_ref ON taxa_ref.id = ref_lu2.id_taxa_ref
          JOIN rubus.taxa_ref_sources src2 ON taxa_ref.source_name = src2.source_name
          GROUP BY ref_lu2.id_taxa_obs
        )

    UNION ALL

    SELECT ref_pref.scientific_name
    FROM rubus.taxa_vernacular AS matched_vernacular
        JOIN rubus.taxa_ref_vernacular_lookup AS vern_lu ON matched_vernacular.id = vern_lu.id_taxa_vernacular
        JOIN rubus.taxa_obs_ref_lookup ref_lu ON vern_lu.id_taxa_ref = ref_lu.id_taxa_ref
        JOIN rubus.taxa_obs_ref_preferred ref_pref ON ref_pref.id_taxa_ref = vern_lu.id_taxa_ref
    WHERE matched_vernacular.name ILIKE $1
        AND matched_vernacular.preferred IS TRUE
        AND ref_lu.match_type IS DISTINCT FROM 'complex'
        AND ref_pref.is_match IS TRUE
)
SELECT distinct on (id_taxa_obs) taxa.*
FROM api.taxa
JOIN match_pref ON taxa.valid_scientific_name = match_pref.scientific_name

ALTER FUNCTION api.match_taxa(text)
    OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------


-- DROP FUNCTION IF EXISTS rubus.match_taxa_groups(integer[]);
CREATE OR REPLACE FUNCTION rubus.match_taxa_groups(
	id_taxa_obs integer[]
)
RETURNS SETOF rubus.taxa_groups AS $$
	with group_id_taxa_obs as (
		select
			id_group,
			array_agg(id_taxa_obs) id_taxa_obs
		from rubus.taxa_obs_group_lookup
		group by id_group
	)
	select rubus.taxa_groups.* from group_id_taxa_obs, rubus.taxa_groups
	where $1 <@ id_taxa_obs
		and id_group = taxa_groups.id
$$ language sql;

ALTER FUNCTION rubus.match_taxa_groups(integer[])
    OWNER TO coleo;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- CREATE FUNCTION taxa_branch_tips that takes a list of id_taxa_obs values and
-- returns the number of unique taxa observed based on the tip-of-the-branch method

-- This function is used by the api.taxa_richness function to compute the number of
-- unique taxa observed based on the tip-of-the-branch method

-- DROP FUNCTION IF EXISTS api.taxa_branch_tips(integer[]);
CREATE OR REPLACE FUNCTION api.taxa_branch_tips (
    taxa_obs_ids integer[]
) RETURNS integer[] AS $$
	with nodes AS (
		select
			id_taxa_ref_valid,
			bool_and((coalesce(match_type = 'complex_closest_parent', false) or is_parent is true) is false) is_tip,
			min(id_taxa_obs) id_taxa_obs,
			count(id_taxa_ref_valid) count_taxa_ref
		from rubus.taxa_obs_ref_lookup obs_lookup
		WHERE obs_lookup.id_taxa_obs = any(taxa_obs_ids)
			and (match_type != 'complex' or match_type is null)
		group by id_taxa_ref_valid
	)
	select array_agg(distinct(id_taxa_obs)) id
	from nodes
	where is_tip is true
$$ LANGUAGE sql;

ALTER FUNCTION api.taxa_branch_tips(integer[])
    OWNER TO coleo;

-- DROP AGGREGATE IF EXISTS api.taxa_branch_tips(integer);
CREATE OR REPLACE AGGREGATE api.taxa_branch_tips (integer) (
	SFUNC = array_append,
	STYPE = integer[],
	FINALFUNC = api.taxa_branch_tips,
	INITCOND = '{}'
);
