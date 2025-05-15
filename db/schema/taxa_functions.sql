--Procedures MATCHING OF SCIENTIFIC NAME
DROP FUNCTION IF EXISTS rubus.match_taxa_obs(text);
CREATE OR REPLACE FUNCTION rubus.match_taxa_obs(
	taxa_name text	
)
-- returns integer[]
RETURNS SETOF taxa_obs AS $$
    with match_taxa_obs as (
        (
            SELECT distinct(match_obs.id_taxa_obs) as id_taxa_obs
            FROM rubus.taxa_ref AS matched_ref
            LEFT JOIN rubus.taxa_obs_ref_lookup AS match_obs
                ON matched_ref.id = match_obs.id_taxa_ref
            WHERE matched_ref.scientific_name ILIKE $1
        ) UNION (
            select distinct(ref_lookup.id_taxa_obs) as id_taxa_obs
            from rubus.taxa_vernacular
            left join rubus.taxa_ref_vernacular_lookup vernacular_lookup
                on taxa_vernacular.id = vernacular_lookup.id_taxa_vernacular
            left join rubus.taxa_obs_ref_lookup ref_lookup
                on vernacular_lookup.id_taxa_ref = ref_lookup.id_taxa_ref
            where taxa_vernacular.name ILIKE $1
        )
    )
    select distinct on (id) taxa_obs.*
    from taxa_obs, match_taxa_obs
    where match_taxa_obs.id_taxa_obs = taxa_obs.id
$$ LANGUAGE sql;

-- -----------------------------------------------------------------------------
-- FUNCTION match_taxa_groups
-- -----------------------------------------------------------------------------

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


GRANT EXECUTE ON FUNCTION rubus.match_taxa_groups(integer[]) TO coleo;
GRANT EXECUTE ON FUNCTION rubus.match_taxa_groups(integer[]) TO read_write_all;