CREATE TABLE IF NOT EXISTS catalogue.genres (
    genre_id   SERIAL
        PRIMARY KEY,
    genre_name VARCHAR(255) NOT NULL
        CONSTRAINT genres_genre_name_uq
            UNIQUE
);

ALTER TABLE catalogue.genres
    OWNER TO showflixadmin;

