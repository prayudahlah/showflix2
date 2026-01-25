def to_show_production_companies(cur, in_paths):
    table = "show_production_companies"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.show_production_companies (show_id, production_company_id)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )
