CREATE SCHEMA IF NOT exists rubus
    AUTHORIZATION coleo;

ALTER SCHEMA rubus OWNER TO coleo;

GRANT ALL ON SCHEMA rubus TO coleo;
GRANT USAGE ON SCHEMA rubus TO read_only_all;
GRANT USAGE ON SCHEMA rubus TO read_only_public;
GRANT USAGE ON SCHEMA rubus TO read_write_all;