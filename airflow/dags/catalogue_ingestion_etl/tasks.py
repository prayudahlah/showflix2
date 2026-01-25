import os
import json
import logging
import psycopg2
import polars as pl
import kagglehub
from airflow.sdk.bases.hook import BaseHook

import catalogue_init_etl.loader as loader
from catalogue_init_etl.null_values import NULL_VALUES

logger = logging.getLogger(__name__)


def extract(
    temp_dir: str,
    dataset_url: str = "asaniczka/tmdb-movies-dataset-2023-930k-movies",
    file_name: str = "TMDB_movie_dataset_v11.csv",
) -> str:
    logger.info("Starting extraction from Kaggle")
    logger.info("Dataset URL: %s", dataset_url)
    logger.info("Target file: %s", file_name)

    os.environ["KAGGLEHUB_CACHE"] = temp_dir
    df_path = kagglehub.dataset_download(dataset_url, path=file_name)

    logger.info("Dataset downloaded successfully")
    logger.info("Extracted data path: %s", df_path)

    return df_path


def transform(in_path: str, temp_dir: str) -> str:
    logger.info("Starting data transformation")
    logger.info("Input path: %s", in_path)

    language_map = pl.scan_csv("./dags/catalogue_init_etl/language_code_mapping.csv")
    out_path = f"{temp_dir}/transform/transformed.csv"

    # map to null, deduplication, language code mapping
    (
        pl.scan_csv(in_path, null_values=NULL_VALUES)
        .with_columns(
            pl.when(pl.col("budget", "revenue", "runtime") == 0)
            .then(None)
            .otherwise(pl.col("budget", "revenue", "runtime"))
            .name.keep(),
        )
        .drop("imdb_id")
        .group_by(pl.col("id"))
        .last(ignore_nulls=True)
        .join(
            language_map,
            how="left",
            left_on="original_language",
            right_on="language_code",
        )
        .with_columns(
            pl.col("language_name").alias("original_language"),
        )
        .rename({"id": "show_id", "adult": "is_adult"})
        .sink_csv(out_path, engine="streaming", mkdir=True)
    )

    # Check if there's any unmapped language code
    unmapped = (
        pl.scan_csv(out_path)
        .select("show_id", "language_name")
        .filter(pl.col("language_name").is_null())
        .unique()
        .collect()
    )

    # raise and return unmapped language if exists
    if unmapped.height > 0:
        unmapped_dict = unmapped.to_dicts()

        logging.error(
            f"Unmapped language code detected:\n{json.dumps(unmapped_dict, indent=4)}"
        )
        raise ValueError("Found unmapped language codes, aborting task.")

    logger.info("CSV loaded into Polars DataFrame")
    logger.info("Data written to parquet")
    logger.info("Output path: %s", out_path)

    return out_path


def normalize(in_path: str, temp_dir: str) -> dict[str, str]:
    logger.info("Starting data normalization")
    logger.info("Input path: %s", in_path)
    out_paths = {}

    # genres
    out_paths["genres"] = f"{temp_dir}/normalize/genres.csv"
    (
        pl.scan_csv(in_path)
        .select(pl.col("genres").str.split(", ").explode().alias("genre_name"))
        .unique()
        .drop_nulls()
        .with_row_index("genre_id", offset=1)
        .sink_csv(out_paths["genres"], engine="streaming", mkdir=True)
    )
    logger.info(f"'genres' table created to {out_paths['genres']}")

    # show_genres
    out_paths["show_genres"] = f"{temp_dir}/normalize/show_genres.csv"
    (
        pl.scan_csv(in_path)
        .select(pl.col("show_id"), pl.col("genres").str.split(", ").alias("genre_name"))
        .drop_nulls()
        .explode("genre_name")
        .join(pl.scan_csv(out_paths["genres"]), how="left", on="genre_name")
        .drop("genre_name")
        .unique()
        .sink_csv(out_paths["show_genres"], engine="streaming", mkdir=True)
    )
    logger.info(f"'show_genres' table created to {out_paths['show_genres']}")

    # production_companies
    out_paths["production_companies"] = f"{temp_dir}/normalize/production_companies.csv"
    (
        pl.scan_csv(in_path)
        .select(
            pl.col("production_companies")
            .str.split(", ")
            .explode()
            .alias("production_company_name")
        )
        .unique()
        .drop_nulls()
        .with_row_index("production_company_id", offset=1)
        .sink_csv(out_paths["production_companies"], engine="streaming", mkdir=True)
    )
    logger.info(
        f"'production_companies' table created to {out_paths['production_companies']}"
    )

    # show_production_companies
    out_paths["show_production_companies"] = (
        f"{temp_dir}/normalize/show_production_companies.csv"
    )
    (
        pl.scan_csv(in_path)
        .select(
            pl.col("show_id"),
            pl.col("production_companies")
            .str.split(", ")
            .alias("production_company_name"),
        )
        .drop_nulls()
        .explode("production_company_name")
        .join(
            pl.scan_csv(out_paths["production_companies"]),
            how="left",
            on="production_company_name",
        )
        .drop("production_company_name")
        .unique()
        .sink_csv(
            out_paths["show_production_companies"], engine="streaming", mkdir=True
        )
    )
    logger.info(
        f"'show_production_companies' table created to {out_paths['show_production_companies']}"
    )

    # regions
    out_paths["regions"] = f"{temp_dir}/normalize/regions.csv"
    (
        pl.scan_csv(in_path)
        .select(
            pl.col("production_countries")
            .str.split(", ")
            .explode()
            .alias("region_name")
        )
        .unique()
        .drop_nulls()
        .with_row_index("region_id", offset=1)
        .sink_csv(out_paths["regions"], engine="streaming", mkdir=True)
    )
    logger.info(f"'regions' table created to {out_paths['regions']}")

    # show_production_regions
    out_paths["show_production_regions"] = (
        f"{temp_dir}/normalize/show_production_regions.csv"
    )
    (
        pl.scan_csv(in_path)
        .select(
            pl.col("show_id"),
            pl.col("production_countries").str.split(", ").alias("region_name"),
        )
        .drop_nulls()
        .explode("region_name")
        .join(pl.scan_csv(out_paths["regions"]), how="left", on="region_name")
        .drop("region_name")
        .unique()
        .sink_csv(out_paths["show_production_regions"], engine="streaming", mkdir=True)
    )
    logger.info(
        f"'show_production_regions' table created to {out_paths['show_production_regions']}"
    )

    # languages
    out_paths["languages"] = f"{temp_dir}/normalize/languages.csv"
    original_lang = pl.scan_csv(in_path).select(
        pl.col("original_language").alias("language_name")
    )
    spoken_lang = pl.scan_csv(in_path).select(
        pl.col("spoken_languages").str.split(", ").explode().alias("language_name")
    )
    (
        pl.concat([original_lang, spoken_lang])
        .unique()
        .drop_nulls()
        .with_row_index("language_id", offset=1)
        .sink_csv(out_paths["languages"], engine="streaming", mkdir=True)
    )
    logger.info(f"'languages' table created to {out_paths['languages']}")

    # show_spoken_languages
    out_paths["show_spoken_languages"] = (
        f"{temp_dir}/normalize/show_spoken_languages.csv"
    )
    (
        pl.scan_csv(in_path)
        .select(
            pl.col("show_id"),
            pl.col("spoken_languages").str.split(", ").alias("language_name"),
        )
        .drop_nulls()
        .explode("language_name")
        .join(pl.scan_csv(out_paths["languages"]), how="left", on="language_name")
        .drop("language_name")
        .unique()
        .sink_csv(out_paths["show_spoken_languages"], engine="streaming", mkdir=True)
    )
    logger.info(
        f"'show_spoken_languages' table created to {out_paths['show_spoken_languages']}"
    )

    # shows
    out_paths["shows"] = f"{temp_dir}/normalize/shows.csv"
    (
        pl.scan_csv(in_path)
        .join(
            pl.scan_csv(out_paths["languages"]),
            how="left",
            left_on="original_language",
            right_on="language_name",
        )
        .with_columns(pl.col("language_id").alias("original_language_id"))
        .drop(
            "language_id",
            "original_language",
            "language_name",
            "genres",
            "production_companies",
            "production_countries",
            "spoken_languages",
        )
        .select(
            "show_id",
            "title",
            "vote_average",
            "vote_count",
            "status",
            "release_date",
            "revenue",
            "runtime",
            "is_adult",
            "backdrop_path",
            "budget",
            "homepage",
            "original_title",
            "overview",
            "popularity",
            "poster_path",
            "tagline",
            "keywords",
            "original_language_id",
        )
        .sink_csv(out_paths["shows"], engine="streaming", mkdir=True)
    )
    logger.info(f"'shows' table created to {out_paths['shows']}")

    logger.info("Dataset normalized successfully")
    logger.info("Local path: %s", json.dumps(out_paths, indent=4))

    return out_paths


def load(in_paths: dict[str, str], extracted_path: str, temp_dir: str) -> str:
    logger.info("Starting load to Postgres")

    postgres_conn = BaseHook.get_connection("showflix_postgres_conn")
    postgres_uri = postgres_conn.get_uri()

    logger.info(
        "Resolved Postgres connection",
        extra={
            "host": postgres_conn.host,
            "schema": postgres_conn.schema,
            "port": postgres_conn.port,
        },
    )

    with psycopg2.connect(postgres_uri) as conn:
        with conn.cursor() as cur:
            loader.to_genres(cur, in_paths)
            loader.to_production_companies(cur, in_paths)
            loader.to_regions(cur, in_paths)
            loader.to_languages(cur, in_paths)

            loader.to_shows(cur, in_paths)

            loader.to_show_genres(cur, in_paths)
            loader.to_show_production_companies(cur, in_paths)
            loader.to_show_production_regions(cur, in_paths)
            loader.to_show_spoken_languages(cur, in_paths)

        conn.commit()

    logger.info("Data successfully written to Postgres")

    return "success"
