SET ROLE coleo;

--  Revoke all privileges on schema rubus from all roles

REVOKE ALL ON SCHEMA rubus FROM PUBLIC;
REVOKE ALL ON SCHEMA rubus FROM read_only_public;
REVOKE ALL ON SCHEMA rubus FROM read_only_all;
REVOKE ALL ON SCHEMA rubus FROM read_write_all;

REVOKE ALL ON ALL TABLES IN SCHEMA rubus FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA rubus FROM read_only_public;
REVOKE ALL ON ALL TABLES IN SCHEMA rubus FROM read_only_all;
REVOKE ALL ON ALL TABLES IN SCHEMA rubus FROM read_write_all;

REVOKE ALL ON ALL SEQUENCES IN SCHEMA rubus FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA rubus FROM read_only_public;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA rubus FROM read_only_all;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA rubus FROM read_write_all;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rubus FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rubus FROM read_only_public;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rubus FROM read_only_all;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rubus FROM read_write_all;

-- GRANT necessary privileges on schema rubus to specific roles
GRANT ALL ON SCHEMA rubus TO coleo; 
GRANT USAGE ON SCHEMA rubus TO read_only_all, read_write_all;

GRANT ALL ON ALL TABLES IN SCHEMA rubus TO coleo;
GRANT SELECT ON ALL TABLES IN SCHEMA rubus TO read_only_all;
GRANT TRUNCATE, INSERT, SELECT, TRIGGER, UPDATE, REFERENCES ON ALL TABLES IN SCHEMA rubus TO read_write_all;

GRANT ALL ON ALL SEQUENCES IN SCHEMA rubus TO coleo;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA rubus TO read_only_all, read_write_all;

GRANT ALL ON ALL FUNCTIONS IN SCHEMA rubus TO coleo;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rubus TO read_only_all, read_write_all;
