from airflow.sdk import dag, task
import pendulum

from stage_1.etl import extract_from_kaggle, transform_data, load_to_postgres


@dag(
    dag_id="etl_to_postgres",
    max_active_runs=1,
    max_active_tasks=10,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    schedule="0 2 * * 0",
    catchup=False,
    tags=["etl", "postgres", "ingestion"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="DAG for data ingestion ETL",
)
def data_ingestion():
    @task
    def extract_stage_1() -> str:
        return extract_from_kaggle()

    @task
    def transform_stage_1(in_path: str) -> str:
        return transform_data(in_path)

    @task
    def load_stage_1(in_path: str):
        load_to_postgres(in_path)

    extracted_path = extract_stage_1()
    cleaned_path = transform_stage_1(extracted_path)
    load_stage_1(cleaned_path)


data_ingestion()
