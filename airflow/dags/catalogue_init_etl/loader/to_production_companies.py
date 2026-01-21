def to_production_companies(cur, in_paths):
    table = "production_companies"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.production_companies (production_company_id, production_company_name)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )

    # Update serial sequance
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('catalogue.production_companies', 'production_company_id'),
            (SELECT COALESCE(MAX(production_company_id), 0) FROM catalogue.production_companies)
        )
        """
    )
