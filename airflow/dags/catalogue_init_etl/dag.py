from airflow.sdk import dag, task
import pendulum
import os

from catalogue_init_etl.tasks import (
    extract_from_kaggle,
    transform_data,
    load_to_postgres,
)


@dag(
    dag_id="catalogue_init_etl",
    max_active_runs=1,
    max_active_tasks=10,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    schedule="0 2 * * 0",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="DAG for data ingestion ETL",
)
def data_ingestion():
    dag_id = "catalogue_init_etl"
    temp_dir = f"{os.getenv('AIRFLOW_TEMP_DIR')}/{dag_id}"

    @task
    def extract_stage_1() -> str:
        return extract_from_kaggle(temp_dir)

    @task
    def transform_stage_1(in_path: str) -> str:
        return transform_data(temp_dir, in_path)

    @task
    def load_stage_1(in_path: str):
        load_to_postgres(in_path)

    extracted_path = extract_stage_1()
    cleaned_path = transform_stage_1(extracted_path)
    load_stage_1(cleaned_path)


data_ingestion()
