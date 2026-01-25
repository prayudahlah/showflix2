def to_shows(cur, in_paths):
    table = "shows"
    in_path = in_paths[table]

    # Copy to postgres table
    with open(in_path, "r") as f:
        cur.copy_expert(
            """
            COPY catalogue.shows (show_id, title, vote_average, vote_count, status, release_date, revenue, runtime, is_adult, backdrop_path, budget, homepage, original_title, overview, popularity, poster_path, tagline, keywords, original_language_id)
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
            """,
            f,
        )
