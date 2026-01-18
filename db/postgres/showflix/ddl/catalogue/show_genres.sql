CREATE TABLE IF NOT EXISTS catalogue.show_genres (
    show_id  INTEGER NOT NULL
        REFERENCES catalogue.shows,
    genre_id INTEGER NOT NULL
        REFERENCES catalogue.genres,
    PRIMARY KEY (show_id, genre_id)
)
    USING ???;

ALTER TABLE catalogue.show_genres
    OWNER TO showflixadmin;

