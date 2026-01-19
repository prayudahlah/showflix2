CREATE TABLE IF NOT EXISTS catalogue.production_companies (
    production_company_id   SERIAL
        PRIMARY KEY,
    production_company_name TEXT NOT NULL
        CONSTRAINT production_companies_production_company_name_uq
            UNIQUE
);

ALTER TABLE catalogue.production_companies
    OWNER TO showflixadmin;

