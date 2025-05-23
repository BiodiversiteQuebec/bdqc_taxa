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
