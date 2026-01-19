CREATE TABLE IF NOT EXISTS catalogue.shows (
    show_id              INTEGER          NOT NULL
        PRIMARY KEY,
    title                TEXT,
    original_title       TEXT,
    original_language_id INTEGER
        REFERENCES catalogue.languages,
    vote_average         NUMERIC(5, 3),
    vote_count           INTEGER,
    popularity           NUMERIC(10, 4),
    status               show_status_enum NOT NULL,
    release_date         DATE,
    budget               BIGINT,
    revenue              BIGINT,
    runtime              INTEGER,
    is_adult             BOOLEAN,
    backdrop_path        VARCHAR(255),
    poster_path          VARCHAR(255),
    homepage             TEXT,
    tagline              TEXT,
    overview             TEXT,
    keywords             TEXT
);

ALTER TABLE catalogue.shows
    OWNER TO showflixadmin;

