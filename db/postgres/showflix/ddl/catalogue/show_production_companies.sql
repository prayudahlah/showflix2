CREATE TABLE IF NOT EXISTS catalogue.show_production_companies (
    show_id               INTEGER NOT NULL
        REFERENCES catalogue.shows,
    production_company_id INTEGER NOT NULL
        REFERENCES catalogue.production_companies,
    PRIMARY KEY (show_id, production_company_id)
)
    USING ???;

ALTER TABLE catalogue.show_production_companies
    OWNER TO showflixadmin;

