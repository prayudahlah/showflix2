def to_show_genres(cur, in_paths):
    table = "show_genres"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.show_genres (show_id, genre_id)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )
