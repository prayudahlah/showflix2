CREATE TABLE IF NOT EXISTS catalogue.regions (
    region_id   SERIAL
        PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL
);

ALTER TABLE catalogue.regions
    OWNER TO showflixadmin;

