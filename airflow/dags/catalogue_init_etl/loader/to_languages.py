def to_languages(cur, in_paths):
    table = "languages"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.languages (language_id, language_name)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )

    # Update serial sequance
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('catalogue.languages', 'language_id'),
            (SELECT COALESCE(MAX(language_id), 0) FROM catalogue.languages)
        )
        """
    )
