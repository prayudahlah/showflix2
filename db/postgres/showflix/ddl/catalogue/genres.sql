CREATE TABLE IF NOT EXISTS catalogue.genres (
    genre_id   SERIAL
        PRIMARY KEY,
    genre_name VARCHAR(255) NOT NULL
);

ALTER TABLE catalogue.genres
    OWNER TO showflixadmin;

