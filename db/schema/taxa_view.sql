SET ROLE coleo;

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
    species.scientific_name as species,
    lemv_ranks.vernacular_fr AS lemv_status,
    sara_ranks.vernacular_fr AS sara_status
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
LEFT JOIN rubus.taxa_obs_ref_preferred species ON species.rank = 'species' AND species.id_taxa_obs = ref_pref.id_taxa_obs
LEFT JOIN 
  (SELECT * FROM rubus.taxa_groups WHERE taxa_groups.short::text IN ('CDPNQ_ENDANGERED', 'CDPNQ_SUSC', 'CDPNQ_VUL', 'CDPNQ_VUL_HARVEST'))
    AS lemv_ranks ON sensitive_group.id_group = lemv_ranks.id
LEFT JOIN 
  (SELECT * FROM rubus.taxa_groups WHERE taxa_groups.short::text IN ('SARA_ENDANGERED', 'SARA_THREATENED', 'SARA_SPECIAL_CONCERN'))
    AS sara_ranks ON sensitive_group.id_group = sara_ranks.id;

ALTER TABLE rubus.taxa_view
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_view TO coleo;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_view TO read_write_all;

COMMENT ON VIEW rubus.taxa_view IS 'A view of taxa observations with preferred scientific names, vernacular names, taxonomic hierarchy, and sensitivity information';
