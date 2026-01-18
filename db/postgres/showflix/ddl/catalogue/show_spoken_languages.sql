CREATE TABLE IF NOT EXISTS catalogue.show_spoken_languages (
    show_id     INTEGER NOT NULL
        REFERENCES catalogue.shows,
    language_id INTEGER NOT NULL
        REFERENCES catalogue.languages,
    PRIMARY KEY (show_id, language_id)
)
    USING ???;

ALTER TABLE catalogue.show_spoken_languages
    OWNER TO showflixadmin;

