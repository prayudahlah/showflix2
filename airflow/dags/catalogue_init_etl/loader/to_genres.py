def to_genres(cur, in_paths):
    table = "genres"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.genres (genre_id, genre_name)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )

    # Update serial sequance
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('catalogue.genres', 'genre_id'),
            (SELECT COALESCE(MAX(genre_id), 0) FROM catalogue.genres)
        )
        """
    )
