-- DROP VIEW rubus.taxa_view;
CREATE OR REPLACE VIEW rubus.taxa_view
AS
SELECT DISTINCT ON (taxa_obs.id)
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
LEFT JOIN rubus.taxa_obs_ref_preferred ref_pref ON taxa_obs.id = ref_pref.id_taxa_obs AND ref_pref.is_match IS TRUE
LEFT JOIN rubus.taxa_ref_vernacular_preferred vernacular_pref ON ref_pref.id_taxa_ref = vernacular_pref.id_taxa_ref
LEFT JOIN 
  (SELECT * FROM rubus.taxa_obs_group_lookup WHERE taxa_obs_group_lookup.short_group::text IN ('SENSITIVE', 'CDPNQ_ENDANGERED', 'CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST'))
    AS sensitive_group USING (id_taxa_obs)
LEFT JOIN 
  (SELECT DISTINCT ON (group_lu.id_taxa_obs) group_lu.id_taxa_obs, taxa_groups.vernacular_fr AS group_fr FROM rubus.taxa_obs_group_lookup group_lu 
    LEFT JOIN rubus.taxa_groups ON group_lu.id_group = taxa_groups.id WHERE taxa_groups.level = 1) AS group_fr USING (id_taxa_obs)
LEFT JOIN
  (SELECT DISTINCT ON (group_lu.id_taxa_obs) group_lu.id_taxa_obs, taxa_groups.vernacular_en AS group_en FROM rubus.taxa_obs_group_lookup group_lu
    LEFT JOIN rubus.taxa_groups ON group_lu.id_group = taxa_groups.id WHERE taxa_groups.level = 1) AS group_en USING (id_taxa_obs)
LEFT JOIN rubus.taxa_obs_ref_preferred kingdom ON kingdom.rank = 'kingdom' AND kingdom.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred phylum ON phylum.rank = 'phylum' AND phylum.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred class ON class.rank = 'class' AND class.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred "order" ON "order".rank = 'order' AND "order".id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred family ON family.rank = 'family' AND family.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred genus ON genus.rank = 'genus' AND genus.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN rubus.taxa_obs_ref_preferred species ON species.rank = 'species' AND species.id_taxa_obs = ref_pref.id_taxa_obs;

ALTER TABLE rubus.taxa_view
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_view TO coleo;
GRANT SELECT ON TABLE rubus.taxa_view TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_view TO read_write_all;

COMMENT ON VIEW rubus.taxa_view IS 'A view of taxa observations with preferred scientific names, vernacular names, taxonomic hierarchy, and sensitivity information';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP FUNCTION IF EXISTS rubus.refresh_taxa();
CREATE OR REPLACE FUNCTION rubus.refresh_taxa()
 RETURNS void
 LANGUAGE plpgsql
AS $BODY$
BEGIN
    DELETE FROM api.taxa;

	INSERT INTO api.taxa
	SELECT * FROM rubus.taxa_view;
END;
$BODY$;

ALTER FUNCTION rubus.refresh_taxa()
    OWNER TO coleo;

GRANT EXECUTE ON FUNCTION rubus.refresh_taxa() TO coleo;
GRANT EXECUTE ON FUNCTION rubus.refresh_taxa() TO read_write_all;
REVOKE ALL ON FUNCTION rubus.refresh_taxa() FROM PUBLIC;

COMMENT ON FUNCTION rubus.refresh_taxa() IS 'Function to refresh the api.taxa table from the rubus.taxa_view view';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS api.taxa_table
CREATE TABLE IF NOT EXISTS api.taxa(
	id_taxa_obs integer NOT NULL,
	observed_scientific_name text NOT NULL,
	valid_scientific_name text,
	rank text,
	sensitive boolean,
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
    OWNER to coleo;

REVOKE ALL ON TABLE api.taxa FROM read_only_all;
REVOKE ALL ON TABLE api.taxa FROM read_write_all;

GRANT ALL ON TABLE api.taxa TO coleo;
GRANT SELECT ON TABLE api.taxa TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE api.taxa TO read_write_all;

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

COMMENT ON TABLE api.taxa IS 'Table to store taxa information refreshed from rubus.taxa_view via rubus.refresh_taxa() function';