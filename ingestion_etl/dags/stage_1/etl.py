import logging
import polars as pl
import kagglehub
from airflow.sdk.bases.hook import BaseHook
from typing import List

logger = logging.getLogger(__name__)


def extract_from_kaggle(
    dataset_url: str = "asaniczka/tmdb-movies-dataset-2023-930k-movies",
    file_name: str = "TMDB_movie_dataset_v11.csv",
) -> str:
    logger.info("Starting extraction from Kaggle")
    logger.info("Dataset URL: %s", dataset_url)
    logger.info("Target file: %s", file_name)

    df_path = kagglehub.dataset_download(dataset_url, path=file_name)

    logger.info("Dataset downloaded successfully")
    logger.info("Local path: %s", df_path)

    return df_path


def clean_data(
    in_path: str,
    null_values: List[str] = [
        "",
        "None",
        "N/A",
        "N/A N/A",
        "NA",
        "-NaN",
        "-nan",
        "<NA>",
        "NULL",
        "NaN",
        "n/a",
        "nan",
        "null",
    ],
) -> str:
    logger.info("Starting data cleaning")
    logger.info("Input path: %s", in_path)
    logger.info("Null values definition: %s", null_values)

    df = pl.scan_csv(in_path, null_values=null_values).collect(engine="streaming")

    logger.info("CSV loaded into Polars DataFrame")
    logger.info("Row count after load: %d", df.height)
    logger.info("Column count: %d", df.width)

    out_path = "/tmp/transformed.parquet"
    df.write_parquet(out_path)

    logger.info("Data written to parquet")
    logger.info("Output path: %s", out_path)

    return out_path


def load_to_postgres(in_path: str, table_name: str = "staging.cleaned_tmdb"):
    logger.info("Starting load to Postgres")

    df = pl.scan_parquet(in_path).collect(engine="streaming")

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
