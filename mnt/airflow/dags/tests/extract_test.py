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


def upload(file_path: str, dataset_name: str) -> str:
    logger.info(f"Lendo o diretorio: {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Carregando no bucket: raw")
    return MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")


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
def extract_test(params=None):


    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/include/{{ params.execution_date }}",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    @task
    def list_staging(date: str) -> dict:
        folder = STAGING_DIR.format(date=date)
        files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
        logger.info(f"Arquivos CSV encontrados: {files}")
        # Retornando como dicionário para mapear tipo de arquivo
        if len(files) < 4:
          raise ValueError("Arquivos insuficientes para o processamento.")
        else:
          return {
              "orders": files[0],
              "payments": files[1],
              "products": files[2],
              "users": files[3]
          }

    @task
    def upload_orders_from_include(files_dict: dict):
        upload(files_dict["orders"], "orders")

    @task
    def upload_payments_from_include(files_dict: dict):
        upload(files_dict["payments"], "payments")

    @task
    def upload_products_from_include(files_dict: dict):
        upload(files_dict["products"], "products")

    @task
    def upload_users_from_include(files_dict: dict):
        upload(files_dict["users"], "users")


    files_task = list_staging("{{ params.execution_date }}")

    orders_task = upload_orders_from_include(files_task)
    payments_task = upload_payments_from_include(files_task)
    products_task = upload_products_from_include(files_task)
    users_task = upload_users_from_include(files_task)

    wait_for_file >> files_task
    files_task >> [orders_task, payments_task, products_task, users_task]
