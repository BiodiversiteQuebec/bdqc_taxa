-- DROP TABLE IF EXISTS rubus.taxa_ref;
CREATE TABLE IF NOT EXISTS rubus.taxa_ref
(
    id integer NOT NULL DEFAULT nextval('rubus.taxa_ref_id_seq'::regclass),
    source_name text  NOT NULL,
    source_id numeric,
    source_record_id text  NOT NULL,
    scientific_name text  NOT NULL,
    authorship text ,
    rank text  NOT NULL,
    valid boolean NOT NULL,
    valid_srid text  NOT NULL,
    classification_srids text[],
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT taxa_ref_pkey PRIMARY KEY (id),
    CONSTRAINT taxa_ref_source_name_source_record_id_key UNIQUE (source_name, source_record_id)
)

ALTER TABLE IF EXISTS rubus.taxa_ref
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_ref FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_ref FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_ref TO coleo;
GRANT SELECT ON TABLE rubus.taxa_ref TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_ref TO read_write_all;

CREATE INDEX IF NOT EXISTS source_id_srid_idx
  ON rubus.taxa_ref (source_id, valid_srid);

CREATE INDEX IF NOT EXISTS scientific_name_idx
  ON rubus.taxa_ref (scientific_name);

CREATE INDEX IF NOT EXISTS taxa_ref_idx
  ON rubus.taxa_ref (id);

CREATE INDEX IF NOT EXISTS taxa_ref_rank_idx
  ON rubus.taxa_ref (rank);

COMMENT ON TABLE rubus.taxa_ref IS 'Reference taxonomy table to store taxonomic names from various sources (e.g., ITIS, GBIF, etc.)';

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_obs_ref_lookup;
CREATE TABLE IF NOT EXISTS rubus.taxa_obs_ref_lookup
(
    id_taxa_obs integer NOT NULL,
    id_taxa_ref integer NOT NULL,
    id_taxa_ref_valid integer NOT NULL,
    match_type text COLLATE pg_catalog."default",
    is_parent boolean,
    CONSTRAINT taxa_obs_ref_lookup_id_taxa_obs_id_taxa_ref_key UNIQUE (id_taxa_obs, id_taxa_ref),
    CONSTRAINT taxa_obs_ref_lookup_id_taxa_obs_fkey FOREIGN KEY (id_taxa_obs)
        REFERENCES public.taxa_obs (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT taxa_obs_ref_lookup_id_taxa_ref_fkey FOREIGN KEY (id_taxa_ref)
        REFERENCES rubus.taxa_ref (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

ALTER TABLE IF EXISTS rubus.taxa_obs_ref_lookup
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_obs_ref_lookup FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_obs_ref_lookup FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_obs_ref_lookup TO coleo;
GRANT SELECT ON TABLE rubus.taxa_obs_ref_lookup TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_obs_ref_lookup TO read_write_all;

CREATE INDEX IF NOT EXISTS id_taxa_obs_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_obs);

CREATE INDEX IF NOT EXISTS id_taxa_ref_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_ref);

CREATE INDEX IF NOT EXISTS id_taxa_ref_valid_idx
    ON rubus.taxa_obs_ref_lookup (id_taxa_ref_valid);

CREATE INDEX IF NOT EXISTS ref_lu_match_type_idx
    ON rubus.taxa_obs_ref_lookup (match_type);

CREATE INDEX IF NOT EXISTS ref_lu_is_parent_idx
    ON rubus.taxa_obs_ref_lookup (is_parent);

CREATE INDEX IF NOT EXISTS id_taxa_obs_id_taxa_ref_valid_composite_covering_idx 
    ON rubus.taxa_obs_ref_lookup (id_taxa_obs, id_taxa_ref_valid) INCLUDE (id_taxa_ref, match_type, is_parent);

COMMENT ON TABLE rubus.taxa_obs_ref_lookup IS 'Lookup table to link taxa_obs records to taxa_ref records';