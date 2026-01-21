def to_show_spoken_languages(cur, in_paths):
    table = "show_spoken_languages"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.show_spoken_languages (show_id, language_id)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )
