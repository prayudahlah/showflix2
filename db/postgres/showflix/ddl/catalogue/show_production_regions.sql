CREATE TABLE IF NOT EXISTS catalogue.show_production_regions (
    show_id   INTEGER NOT NULL
        REFERENCES catalogue.shows,
    region_id INTEGER NOT NULL
        REFERENCES catalogue.regions,
    PRIMARY KEY (show_id, region_id)
)
    USING ???;

ALTER TABLE catalogue.show_production_regions
    OWNER TO showflixadmin;

