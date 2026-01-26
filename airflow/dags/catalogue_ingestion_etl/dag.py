from airflow.sdk import dag, task, chain, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from pathlib import Path
import polars as pl
import pendulum
import logging

from catalogue_ingestion_etl.utils import get_s3_storage_options, get_kaggle_dataset

logger = logging.getLogger(__name__)


@dag(
    dag_id="catalogue_ingestion_etl",
    max_active_runs=1,
    max_active_tasks=10,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="DAG for data ingestion ETL",
)
def catalogue_ingestion_etl():
    start = EmptyOperator(task_id="start")

    @task
    def extract_from_kaggle(**context) -> str:
        temp_dir = Path(Variable.get("AIRFLOW_TEMP_DIR", default="/opt/airflow/tmp"))
        data_interval_start = context["data_interval_start"]
        date_prefix = data_interval_start.strftime("year=%Y/month=%m/day=%d")

        with get_kaggle_dataset(temp_dir) as df_path:
            try:
                bucket = Variable.get("CATALOGUE_INGESTION_ETL_BUCKET")
            except KeyError:
                raise RuntimeError(
                    "Airflow variable CATALOGUE_INGESTION_ETL_BUCKET needs to be set"
                )

            s3_uri = f"s3://{bucket}/{date_prefix}/kaggle_dataset.parquet"
            storage_options = get_s3_storage_options()

            logger.info(f"Uploading raw dataset to '{s3_uri}'")

            pl.scan_csv(df_path).sink_parquet(s3_uri, storage_options=storage_options)

            logger.info(f"Succesfully uploaded raw dataset to '{s3_uri}'")

        return s3_uri

    extract_from_kaggle = extract_from_kaggle()

    end = EmptyOperator(task_id="end")

    chain(start, extract_from_kaggle, end)


catalogue_ingestion_etl()
