import os
from typing import List
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.utils.log.logging_mixin import LoggingMixin

import pandas as pd
import boto3
from botocore.client import Config
from python.minio import MinioUtils

# Logger integrado ao Airflow
logger = LoggingMixin().log

ENDPOINT_URL = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

S3_CLIENT = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
MINIO = MinioUtils(S3_CLIENT)

STAGING_DIR = "/opt/airflow/include/{date}"

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "minio", "ingestion", "csv", "pyspark", "postgres"],
    params={"execution_date": datetime.today().strftime('%Y-%m-%d')}
)
def ecommerce_etl(params=None):

    # TaskGroup: Extract
    with TaskGroup("extract", tooltip="Extração e upload CSV -> MinIO") as extract_group:

        wait_for_file = FileSensor(
            task_id="wait_for_file",
            filepath="/opt/airflow/include/{{ params.execution_date }}",
            fs_conn_id="fs_default",
            poke_interval=60,
            timeout=60 * 60,
            mode="reschedule",
        )

        @task
        def list_csv_files(date: str) -> List[str]:
            folder = STAGING_DIR.format(date=date)
            files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
            logger.info(f"Arquivos CSV encontrados: {files}")
            return files

        @task
        def upload_orders(file_path: str):
            logger.info(f"Processando: {file_path}")
            df = pd.read_csv(file_path)
            dataset_name = os.path.basename(file_path).replace(".csv", "")
            MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")

        @task
        def upload_payments(file_path: str):
            logger.info(f"Processando: {file_path}")
            df = pd.read_csv(file_path)
            dataset_name = os.path.basename(file_path).replace(".csv", "")
            MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")

        @task
        def upload_products(file_path: str):
            logger.info(f"Processando: {file_path}")
            df = pd.read_csv(file_path)
            dataset_name = os.path.basename(file_path).replace(".csv", "")
            MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")

        @task
        def upload_users(file_path: str):
            logger.info(f"Processando: {file_path}")
            df = pd.read_csv(file_path)
            dataset_name = os.path.basename(file_path).replace(".csv", "")
            MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")


        # não executar agora, só criar a task
        files_task = list_csv_files("{{ params.execution_date }}")

        wait_for_file >> files_task
        files_task >> upload_file_to_minio.partial().expand(file_path=files_task)
