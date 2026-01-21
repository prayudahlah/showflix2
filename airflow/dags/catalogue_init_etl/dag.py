from airflow.sdk import dag, task
import pendulum
import os

import catalogue_init_etl.tasks as tasks


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
    def extract() -> str:
        return tasks.extract(temp_dir)

    @task
    def transform(in_path: str) -> str:
        return tasks.transform(in_path, temp_dir)

    @task
    def normalize(in_path: str) -> dict[str, str]:
        return tasks.normalize(in_path, temp_dir)

    @task
    def load(in_paths: dict[str, str], extracted_path: str):
        return tasks.load(in_paths, extracted_path, temp_dir)

    extract_res = extract()
    transform_res = transform(extract_res)
    normalize_res = normalize(transform_res)
    load(normalize_res, extract_res)


data_ingestion()
