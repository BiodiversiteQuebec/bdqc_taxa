-- DROP MATERIALIZED VIEW IF EXISTS rubus.taxa_obs_ref_preferred;
CREATE MATERIALIZED VIEW IF NOT EXISTS rubus.taxa_obs_ref_preferred AS
WITH all_ref AS (
    SELECT DISTINCT ON (ref_lu.id_taxa_obs, taxa_ref.id)
        ref_lu.id_taxa_obs,
        taxa_ref.id AS id_taxa_ref,
        taxa_ref.scientific_name AS scientific_name,
        taxa_ref."rank",
        COALESCE(taxa_ref_sources.source_priority, 9999) AS source_priority,
        ref_lu.match_type,
        taxa_ref.source_name,
        taxa_ref.valid,
        ref_lu.is_parent,
        taxa_rank_order."order" as rank_order
    FROM rubus.taxa_obs_ref_lookup ref_lu
    LEFT JOIN rubus.taxa_ref ON ref_lu.id_taxa_ref_valid = taxa_ref.id
    LEFT JOIN rubus.taxa_ref_sources USING (source_id)
    LEFT JOIN rubus.taxa_rank_order on taxa_ref."rank" = taxa_rank_order.rank_name
    WHERE COALESCE(ref_lu.match_type, ''::text) <> 'complex'::text
), is_match AS (
    SELECT DISTINCT ON (id_taxa_obs)
        id_taxa_obs,
        "rank",
        rank_order,
        TRUE AS is_match
    FROM all_ref
    WHERE is_parent IS false OR match_type IS NOT NULL -- Second conditions only for higher rank matches (`higherrank`, `closest common parent`)
    ORDER BY id_taxa_obs, source_priority, is_parent, scientific_name, id_taxa_ref
)
SELECT DISTINCT ON (all_ref.id_taxa_obs, all_ref."rank")
    all_ref.id_taxa_obs,
    all_ref.id_taxa_ref,
    all_ref."rank",
    COALESCE(is_match, FALSE) AS is_match,
    all_ref.scientific_name,
    all_ref.source_name
FROM all_ref
JOIN (
    SELECT id_taxa_obs, MAX(rank_order) AS max_rank_order
    FROM is_match
    GROUP BY id_taxa_obs
) max_ranks
  ON all_ref.id_taxa_obs = max_ranks.id_taxa_obs
LEFT JOIN is_match ON all_ref.id_taxa_obs = is_match.id_taxa_obs AND all_ref."rank" = is_match."rank"
WHERE COALESCE(all_ref.rank_order, 0 ) <= max_ranks.max_rank_order
ORDER BY all_ref.id_taxa_obs, all_ref."rank", all_ref.source_priority, all_ref.scientific_name, all_ref.id_taxa_ref
WITH DATA;

ALTER TABLE IF EXISTS rubus.taxa_obs_ref_preferred
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_obs_ref_preferred TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_ref_preferred TO read_only_all;
GRANT SELECT ON TABLE rubus.taxa_obs_ref_preferred TO read_write_all;

CREATE INDEX id_taxa_obs_rank_match_idx ON rubus.taxa_obs_ref_preferred(id_taxa_obs, rank, is_match);
CREATE INDEX id_taxa_ref_match_idx ON rubus.taxa_obs_ref_preferred(id_taxa_ref, is_match);

COMMENT ON MATERIALIZED VIEW rubus.taxa_obs_ref_preferred IS 'Materialized view of preferred taxonomic references for taxa_obs, selecting the best matching reference based on predefined criteria.';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

--DROP MATERIALIZED VIEW IF EXISTS rubus.taxa_ref_vernacular_preferred;
CREATE MATERIALIZED VIEW IF NOT EXISTS rubus.taxa_ref_vernacular_preferred AS
WITH all_vernacular AS (
    SELECT DISTINCT ON (v_lu.id_taxa_ref, tv.rank, tv.language)
        v_lu.id_taxa_ref,
        tv.id AS id_taxa_vernacular,
        tv.name,
        tv."rank",
        tv.language,
        true AS is_match,
        tv.preferred,
        tv.source_name
    FROM rubus.taxa_ref_vernacular_lookup v_lu
        LEFT JOIN rubus.taxa_ref ON v_lu.id_taxa_ref = taxa_ref.id 
        LEFT JOIN rubus.taxa_vernacular tv ON v_lu.id_taxa_vernacular = tv.id AND tv.rank = taxa_ref.rank
        LEFT JOIN rubus.taxa_vernacular_sources src ON tv.source_name = src.source_name::text
    WHERE tv.id IS NOT NULL
    ORDER BY v_lu.id_taxa_ref, tv."rank", tv.language, src.source_priority, tv.preferred DESC, tv.name
), vernacular_eng AS (
  SELECT all_vernacular.id_taxa_ref,
      all_vernacular.name AS vernacular_en,
      all_vernacular.id_taxa_vernacular AS id_taxa_vernacular_en,
      all_vernacular."rank" AS rank_en,
      all_vernacular.is_match,
      all_vernacular.preferred AS preferred_en,
      all_vernacular.source_name AS source_en
  FROM all_vernacular
  WHERE all_vernacular.language = 'eng'::text
), vernacular_fra AS (
  SELECT all_vernacular.id_taxa_ref,
      all_vernacular.name AS vernacular_fr,
      all_vernacular.id_taxa_vernacular AS id_taxa_vernacular_fr,
      all_vernacular."rank" AS rank_fr,
      all_vernacular.is_match,
      all_vernacular.preferred AS preferred_fr,
      all_vernacular.source_name AS source_fr
  FROM all_vernacular
  WHERE all_vernacular.language = 'fra'::text
)
  SELECT COALESCE(vernacular_eng.id_taxa_ref, vernacular_fra.id_taxa_ref) AS id_taxa_ref,
      vernacular_eng.id_taxa_vernacular_en,
      vernacular_fra.id_taxa_vernacular_fr,
      vernacular_eng.vernacular_en,
      vernacular_fra.vernacular_fr,
      vernacular_eng.rank_en AS "rank",
      COALESCE(vernacular_eng.is_match, vernacular_fra.is_match) AS is_match,
      vernacular_eng.source_en,
      vernacular_fra.source_fr
  FROM vernacular_eng
    FULL JOIN vernacular_fra USING (id_taxa_ref)
WITH DATA;

ALTER TABLE IF EXISTS rubus.taxa_ref_vernacular_preferred
    OWNER TO coleo;

GRANT ALL ON TABLE rubus.taxa_ref_vernacular_preferred TO coleo;
GRANT SELECT ON TABLE rubus.taxa_ref_vernacular_preferred TO read_only_all;
GRANT SELECT ON TABLE rubus.taxa_ref_vernacular_preferred TO read_write_all;

CREATE INDEX ON rubus.taxa_ref_vernacular_preferred (id_taxa_ref);
CREATE INDEX ON rubus.taxa_ref_vernacular_preferred (id_taxa_vernacular_en);
CREATE INDEX ON rubus.taxa_ref_vernacular_preferred (id_taxa_vernacular_fr);
CREATE INDEX ON rubus.taxa_ref_vernacular_preferred ("rank");

COMMENT ON MATERIALIZED VIEW rubus.taxa_ref_vernacular_preferred IS 'Materialized view of preferred vernacular names for taxonomic references, selecting the best matching vernacular based on predefined criteria.';