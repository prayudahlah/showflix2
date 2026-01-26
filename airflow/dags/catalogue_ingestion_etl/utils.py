import subprocess
import logging
from contextlib import contextmanager
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import Variable
from pathlib import Path

logger = logging.getLogger(__name__)


def get_s3_storage_options():
    conn = BaseHook.get_connection("garage_s3_conn")

    storage_options = {
        "aws_access_key_id": conn.login,
        "aws_secret_access_key": conn.password,
        "aws_region": conn.extra_dejson.get("aws_region"),
        "aws_endpoint_url": conn.extra_dejson.get("aws_endpoint_url"),
        "aws_allow_http": "true",
    }

    return storage_options


@contextmanager
def get_kaggle_dataset(temp_dir: Path):
    dataset_url = Variable.get(
        "KAGGLE_DATASET_URL",
        default="asaniczka/tmdb-movies-dataset-2023-930k-movies",
    )
    dataset_file_name = Variable.get(
        "KAGGLE_DATASET_FILE_NAME", default="TMDB_movie_dataset_v11.csv"
    )
    out_path = temp_dir / "kaggle_dataset"
    out_path.mkdir(exist_ok=True)
    cmd = [
        "kaggle",
        "datasets",
        "download",
        "-d",
        dataset_url,
        "-f",
        dataset_file_name,
        "-p",
        str(out_path),
        "-q",
    ]

    try:
        logger.info(f"Downloading dataset '{dataset_url}/{dataset_file_name}'")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.debug(f"Kaggle CLI output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Kaggle download failed. Stderr: {e.stderr}")
        raise RuntimeError(
            f"Failed to download Kaggle dataset {dataset_url}: {e}"
        ) from e

    out_path = out_path / dataset_file_name

    try:
        if not out_path.exists():
            raise FileNotFoundError(f"Target file not found at: {out_path}")

        yield out_path
    finally:
        logger.info(f"Cleaning up temporary file: {out_path}")
        if out_path.exists():
            try:
                out_path.unlink()
            except OSError as e:
                logger.warning(f"Could not remove {out_path}: {e}")
