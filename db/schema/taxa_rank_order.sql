-- -----------------------------------------------------
-- Table rubus.taxa_rank_order
-- DESCRIPTION: This table contains the ordered list of ranks for taxa data
-- -----------------------------------------------------
-- Table: rubus.taxa_rank_order

-- DROP TABLE IF EXISTS rubus.taxa_rank_order;

CREATE TABLE IF NOT EXISTS rubus.taxa_rank_order
(
    rank_name text COLLATE pg_catalog."default" NOT NULL,
    "order" integer NOT NULL,
    CONSTRAINT taxa_rank_priority_pkey PRIMARY KEY (rank_name),
    CONSTRAINT taxa_rank_order_rank_name_order_key UNIQUE (rank_name, "order")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS rubus.taxa_rank_order
    OWNER to coleo;

CREATE INDEX IF NOT EXISTS taxa_rank_order_rank_name_idx
  ON rubus.taxa_rank_order (rank_name);

REVOKE ALL ON TABLE rubus.taxa_rank_order FROM read_write_all;

GRANT ALL ON TABLE rubus.taxa_rank_order TO coleo;

GRANT INSERT, REFERENCES, SELECT, TRIGGER, TRUNCATE, UPDATE ON TABLE rubus.taxa_rank_order TO read_write_all;


-- Insert data
insert into rubus.taxa_rank_order (rank_name, "order") values ('kingdom', 0);
insert into rubus.taxa_rank_order (rank_name, "order") values ('phylum', 1);
insert into rubus.taxa_rank_order (rank_name, "order") values ('class', 2);
insert into rubus.taxa_rank_order (rank_name, "order") values ('order', 3);
insert into rubus.taxa_rank_order (rank_name, "order") values ('family', 4);
insert into rubus.taxa_rank_order (rank_name, "order") values ('genus', 5);
insert into rubus.taxa_rank_order (rank_name, "order") values ('species', 6);
insert into rubus.taxa_rank_order (rank_name, "order") values ('subspecies', 7);
insert into rubus.taxa_rank_order (rank_name, "order") values ('variety', 8);