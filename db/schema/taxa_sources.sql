SET ROLE coleo;

-- DROP TABLE IF EXISTS rubus.taxa_ref_sources;
CREATE TABLE IF NOT EXISTS rubus.taxa_ref_sources (
  source_id INTEGER PRIMARY KEY,
  source_name VARCHAR(255) NOT NULL,
  source_priority INTEGER NOT NULL,
  id_datasets uuid NOT NULL
  CONSTRAINT taxa_ref_source_id_datasets_fkey FOREIGN KEY (id_datasets)
    REFERENCES public.datasets (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION,
);

ALTER TABLE IF EXISTS rubus.taxa_ref_sources
    OWNER to coleo;

GRANT ALL ON TABLE rubus.taxa_ref_sources TO coleo;
GRANT SELECT ON TABLE rubus.taxa_ref_sources TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_ref_sources TO read_write_all;

COMMENT ON TABLE rubus.taxa_ref_sources IS 'Rank priority of reference sources for taxonomic data. Lower number indicates higher priority.';

DELETE FROM rubus.taxa_ref_sources;

INSERT INTO rubus.taxa_ref_sources
VALUES (1002, 'CDPNQ', 1, '9b779078-1fd1-4492-8bbe-0892b0d13192'),
	(1001, 'Bryoquel', 2, 'e2178209-373b-4370-9ef4-f0b4bc964b40'),
	(147, 'VASCAN', 3, '3f8a1297-3259-4700-91fc-acc4170b27ce'),
  	(11, 'GBIF Backbone Taxonomy', 4, 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'),
	(3, 'ITIS', 5, '9ca92552-f23a-41a8-a140-01abaa31c931'),
	(1, 'Catalogue of Life', 6, '7ddf754f-d193-4cc9-b351-99906754a03b');

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_vernacular_sources;
CREATE TABLE IF NOT EXISTS rubus.taxa_vernacular_sources(
	source_name VARCHAR(255) PRIMARY KEY,
	source_priority INTEGER NOT NULL
);

ALTER TABLE IF EXISTS rubus.taxa_vernacular_sources
    OWNER to coleo;

GRANT ALL ON TABLE rubus.taxa_vernacular_sources TO coleo;
GRANT SELECT ON TABLE rubus.taxa_vernacular_sources TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_vernacular_sources TO read_write_all;

COMMENT ON TABLE rubus.taxa_vernacular_sources IS 'Rank priority of vernacular name sources for taxonomic data. Lower number indicates higher priority.';

DELETE FROM rubus.taxa_vernacular_sources;

INSERT INTO rubus.taxa_vernacular_sources
VALUES ('CDPNQ', 1),
	('Eliso', 2),
	('Bryoquel', 3),
	('Database of Vascular Plants of Canada (VASCAN)', 4),
	('Integrated Taxonomic Information System (ITIS)', 5),
	('Checklist of Vermont Species', 6);
