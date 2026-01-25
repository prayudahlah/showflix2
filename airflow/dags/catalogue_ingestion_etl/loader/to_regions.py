def to_regions(cur, in_paths):
    table = "regions"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.regions (region_id, region_name)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )

    # Update serial sequance
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('catalogue.regions', 'region_id'),
            (SELECT COALESCE(MAX(region_id), 0) FROM catalogue.regions)
        )
        """
    )
