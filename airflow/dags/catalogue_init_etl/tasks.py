import os
import json
import logging
import polars as pl
import kagglehub
from airflow.sdk.bases.hook import BaseHook
from catalogue_init_etl.null_values import NULL_VALUES

logger = logging.getLogger(__name__)


def extract_from_kaggle(
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
    logger.info("Local path: %s", df_path)

    return df_path


def transform_data(in_path: str, temp_dir: str) -> str:
    logger.info("Starting data transformation")
    logger.info("Input path: %s", in_path)

    language_map = pl.scan_csv("./dags/catalogue_init_etl/language_code_mapping.csv")

    # map to null, deduplication, language code mapping
    df = (
        pl.scan_csv(in_path, null_values=NULL_VALUES)
        .with_columns(
            pl.when(pl.col("budget", "revenue", "runtime") == 0)
            .then(None)
            .otherwise(pl.col("budget", "revenue", "runtime"))
            .name.keep()
        )
        .group_by(pl.col("id"))
        .last(ignore_nulls=True)
        .join(
            language_map,
            how="left",
            left_on="original_language",
            right_on="language_code",
        )
        .collect(engine="streaming")
    )

    # Check if there's any unmapped language code
    unmapped = (
        df.select("id", "original_language", "language_name")
        .filter(pl.col("language_name").is_null())
        .unique()
    )

    # raise and return unmapped language if exists
    if unmapped.height > 0:
        unmapped_dict = unmapped.to_dicts()

        logging.error(
            f"Unmapped language code detected:\n{json.dumps(unmapped_dict, indent=4)}"
        )
        raise ValueError("Found unmapped language codes, aborting task.")

    logger.info("CSV loaded into Polars DataFrame")
    logger.info("Row count after transform: %d", df.height)
    logger.info("Column count: %d", df.width)

    out_path = temp_dir
    df.write_parquet(out_path)

    logger.info("Data written to parquet")
    logger.info("Output path: %s", out_path)

    return out_path


def load_to_postgres(in_path: str, table_name: str = "staging.cleaned_tmdb"):
    logger.info("Starting load to Postgres")

    df = (
        pl.scan_parquet(in_path, try_parse_hive_dates=True)
        .with_columns(
            pl.col("id").cast(pl.Int32).alias("show_id"),
            pl.col("vote_average").cast(pl.Float32),
            pl.col("vote_count").cast(pl.Int32),
            pl.col("popularity").cast(pl.Float32),
            pl.col("runtime").cast(pl.Int32),
            pl.col("language_name").alias("original_language"),
        )
        .drop("id", "language_name")
        .collect(engine="streaming")
    )

    postgres_conn = BaseHook.get_connection("staging_postgres_conn")
    postgres_uri = postgres_conn.get_uri()

    logger.info(
        "Resolved Postgres connection",
        extra={
            "host": postgres_conn.host,
            "schema": postgres_conn.schema,
            "port": postgres_conn.port,
        },
    )

    df.write_database(
        table_name=table_name,
        connection=postgres_uri,
        if_table_exists="replace",
        engine="adbc",
    )

    logger.info("Data successfully written to Postgres")
    logger.info(f"Target table: {table_name}")
