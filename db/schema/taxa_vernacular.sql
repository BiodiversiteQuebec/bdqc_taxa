-- DROP TABLE IF EXISTS rubus.taxa_vernacular;
CREATE TABLE IF NOT EXISTS rubus.taxa_vernacular
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

ALTER TABLE IF EXISTS rubus.taxa_vernacular
    OWNER to coleo;

CREATE INDEX IF NOT EXISTS taxa_vernacular_source_name_idx
  ON rubus.taxa_vernacular (source_name);

CREATE INDEX IF NOT EXISTS taxa_vernacular_source_record_id_idx
    ON rubus.taxa_vernacular (source_record_id);

CREATE INDEX IF NOT EXISTS taxa_vernacular_language_idx
    ON rubus.taxa_vernacular (language);

CREATE INDEX IF NOT EXISTS taxa_vernacular_name_idx
    ON rubus.taxa_vernacular (name);

CREATE INDEX IF NOT EXISTS taxa_vernacular_rank_order_idx
    ON rubus.taxa_vernacular (rank_order);

-- Trigger: update_modified_at
-- DROP TRIGGER IF EXISTS update_modified_at ON rubus.taxa_vernacular;
CREATE OR REPLACE TRIGGER update_modified_at
    BEFORE UPDATE 
    ON rubus.taxa_vernacular
    FOR EACH ROW
    EXECUTE FUNCTION rubus.trigger_set_timestamp();

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_ref_vernacular_lookup;
CREATE TABLE IF NOT EXISTS rubus.taxa_ref_vernacular_lookup
(
    id_taxa_ref integer NOT NULL,
    id_taxa_vernacular integer NOT NULL,
    CONSTRAINT taxa_ref_vernacular_lookup_id_taxa_ref_id_taxa_vernacular_key UNIQUE (id_taxa_ref, id_taxa_vernacular)
);

ALTER TABLE IF EXISTS rubus.taxa_ref_vernacular_lookup 
    OWNER to coleo;

CREATE INDEX taxa_ref_vernacular_lookup_id_taxa_ref_idx ON rubus.taxa_ref_vernacular_lookup (id_taxa_ref);
CREATE INDEX taxa_ref_vernacular_lookup_id_taxa_vernacular_idx ON rubus.taxa_ref_vernacular_lookup (id_taxa_vernacular);
