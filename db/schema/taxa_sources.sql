-- DROP TABLE IF EXISTS rubus.taxa_ref_sources;
CREATE TABLE IF NOT EXISTS rubus.taxa_ref_sources (
  source_id INTEGER PRIMARY KEY,
  source_name VARCHAR(255) NOT NULL,
  source_priority INTEGER NOT NULL
);

ALTER TABLE IF EXISTS rubus.taxa_ref_sources
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_ref_sources FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_ref_sources FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_ref_sources TO coleo;
GRANT SELECT ON TABLE rubus.taxa_ref_sources TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON TABLE rubus.taxa_ref_sources TO read_write_all;

COMMENT ON TABLE rubus.taxa_ref_sources IS 'Rank priority of reference sources for taxonomic data. Lower number indicates higher priority.';

DELETE FROM rubus.taxa_ref_sources;

INSERT INTO rubus.taxa_ref_sources
VALUES (1002, 'CDPNQ', 1),
	(1001, 'Bryoquel', 2),
	(147, 'VASCAN', 3),
  	(11, 'GBIF Backbone Taxonomy', 4),
	(3, 'ITIS', 5),
	(1, 'Catalogue of Life', 6);

--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- DROP TABLE IF EXISTS rubus.taxa_vernacular_sources;
CREATE TABLE IF NOT EXISTS rubus.taxa_vernacular_sources(
	source_name VARCHAR(255) PRIMARY KEY,
	source_priority INTEGER NOT NULL
);

ALTER TABLE IF EXISTS rubus.taxa_vernacular_sources
    OWNER to coleo;

REVOKE ALL ON TABLE rubus.taxa_vernacular_sources FROM read_only_all;
REVOKE ALL ON TABLE rubus.taxa_vernacular_sources FROM read_write_all;

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
