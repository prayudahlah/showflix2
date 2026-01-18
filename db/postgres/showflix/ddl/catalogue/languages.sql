CREATE TABLE IF NOT EXISTS catalogue.languages (
    language_id   SERIAL
        PRIMARY KEY,
    language_name VARCHAR(255) NOT NULL
        CONSTRAINT languages_language_name_uq
            UNIQUE
)
    USING ???;

ALTER TABLE catalogue.languages
    OWNER TO showflixadmin;

