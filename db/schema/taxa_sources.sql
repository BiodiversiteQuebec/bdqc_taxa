-- DROP TABLE IF EXISTS rubus.taxa_ref_sources;
CREATE TABLE IF NOT EXISTS rubus.taxa_ref_sources (
  source_id INTEGER PRIMARY KEY,
  source_name VARCHAR(255) NOT NULL,
  source_priority INTEGER NOT NULL
);

ALTER TABLE IF EXISTS rubus.taxa_ref_sources
    OWNER to coleo;

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

DELETE FROM rubus.taxa_vernacular_sources;

INSERT INTO rubus.taxa_vernacular_sources
VALUES ('CDPNQ', 1),
	('Eliso', 2),
	('Bryoquel', 3),
	('Database of Vascular Plants of Canada (VASCAN)', 4),
	('Integrated Taxonomic Information System (ITIS)', 5),
	('Checklist of Vermont Species', 6);
