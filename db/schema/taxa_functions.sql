-- DROP FUNCTION IF EXISTS rubus.match_taxa(text);
CREATE OR REPLACE FUNCTION rubus.match_taxa(
	taxa_name text)
    RETURNS TABLE (id_taxa_obs integer) 
    LANGUAGE 'sql'
    STABLE PARALLEL SAFE
AS $BODY$
  WITH matched_taxa_obs AS (
      SELECT DISTINCT id_taxa_obs
      FROM rubus.taxa_ref mref
      JOIN rubus.taxa_obs_ref_lookup mlu
          ON mref.id = mlu.id_taxa_ref
          AND is_parent IS false
          AND match_type IS NOT NULL
          AND match_type <> 'complex'
      WHERE mref.scientific_name ILIKE $1
  ), children_taxa_obs AS (
      SELECT DISTINCT id_taxa_obs
      FROM rubus.taxa_ref mref
      JOIN rubus.taxa_obs_ref_lookup mlu
          ON mref.id = mlu.id_taxa_ref
          AND is_parent IS true
      WHERE mref.scientific_name ILIKE $1
  ), pref_synonyms AS (
      SELECT DISTINCT syn_pref.id_taxa_obs
      FROM (
           SELECT id_taxa_obs FROM matched_taxa_obs
             UNION
            SELECT id_taxa_obs FROM children_taxa_obs
            ) AS all_obs
      JOIN rubus.taxa_obs_ref_preferred mpref
          ON all_obs.id_taxa_obs = mpref.id_taxa_obs
          AND mpref.is_match
      JOIN rubus.taxa_obs_ref_preferred syn_pref
          ON mpref.id_taxa_ref = syn_pref.id_taxa_ref
          AND syn_pref.is_match IS true
  )
  SELECT id_taxa_obs
  FROM pref_synonyms
$BODY$;

ALTER FUNCTION rubus.match_taxa(text)
    OWNER TO coleo;

COMMENT ON FUNCTION rubus.match_taxa(text) IS 'Returns taxa matching the given scientific name, including synonyms and child taxa, to feed functions for portal';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP FUNCTION IF EXISTS rubus.match_taxa_groups(integer[]);
CREATE OR REPLACE FUNCTION rubus.match_taxa_groups(
	id_taxa_obs integer[])
    RETURNS SETOF rubus.taxa_groups 
    LANGUAGE 'sql'
AS $BODY$
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
$BODY$;

ALTER FUNCTION rubus.match_taxa_groups(integer[])
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION rubus.match_taxa_groups(integer[]) TO coleo;
GRANT EXECUTE ON FUNCTION rubus.match_taxa_groups(integer[]) TO read_write_all;
REVOKE ALL ON FUNCTION rubus.match_taxa_groups(integer[]) FROM PUBLIC;

COMMENT ON FUNCTION rubus.match_taxa_groups(integer[]) IS 'Returns taxa groups matching the given list of id_taxa_obs';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- CREATE FUNCTION taxa_branch_tips that takes a list of id_taxa_obs values and
-- returns the number of unique taxa observed based on the tip-of-the-branch method

-- This function is used by the api.taxa_richness function to compute the number of
-- unique taxa observed based on the tip-of-the-branch method

-- DROP FUNCTION IF EXISTS rubus.taxa_branch_tips(integer[]);
CREATE OR REPLACE FUNCTION rubus.taxa_branch_tips (
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

ALTER FUNCTION rubus.taxa_branch_tips(integer[])
    OWNER TO coleo;

-- DROP AGGREGATE IF EXISTS rubus.taxa_branch_tips(integer);
CREATE OR REPLACE AGGREGATE rubus.taxa_branch_tips (integer) (
	SFUNC = array_append,
	STYPE = integer[],
	FINALFUNC = rubus.taxa_branch_tips,
	INITCOND = '{}'
);

COMMENT ON FUNCTION rubus.taxa_branch_tips(integer[]) IS 'Returns the list of id_taxa_obs corresponding to the tip-of-the-branch method for the given list of id_taxa_obs';

ALTER AGGREGATE rubus.taxa_branch_tips(integer)
    OWNER TO coleo;