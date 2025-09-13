import os
from typing import List
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
        def upload_file_to_minio(file_path: str):
            logger.info(f"Processando: {file_path}")
            df = pd.read_csv(file_path)
            dataset_name = os.path.basename(file_path).replace(".csv", "")
            MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")

        files = list_csv_files("{{ params.execution_date }}")
        wait_for_file >> files >> upload_file_to_minio.partial().expand(file_path=files)

    # TaskGroup: Transform
    with TaskGroup("transform", tooltip="Transformação PySpark e carga processed") as transform_group:

        @task
        def list_raw_files() -> List[str]:
            files = MINIO.list_raw_objects()
            logger.info(f"Arquivos encontrados no bucket raw: {files}")
            
            if isinstance(files, str):
                files = [files]
            return files

        raw_files = list_raw_files()

        @task
        def build_spark_args(files: List[str]) -> List[List[str]]:
            return [[f] for f in files]

        spark_args = build_spark_args(raw_files)

        spark_task = SparkSubmitOperator.partial(
            task_id="spark_submit_task",
            application="/opt/airflow/dags/spark/processing.py",
            conn_id="spark_default",
            conf={
                "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT"),
                "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
                "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
            verbose=True,
        ).expand(application_args=spark_args)

    # TaskGroup: Validation
    with TaskGroup("validation", tooltip="Great Expectations validation results") as validation_group:

        @task
        def check_validation(table: str):
            result = MINIO.object_validation(table)
            if not result["success"]:
                raise ValueError(f"Validação falhou para {table}")

        table_list = ["orders", "payments", "products", "users"]
        check_validation.partial().expand(table=table_list)

    # Sequência do DAG
    extract_group >> transform_group >> validation_group

dag = ecommerce_etl()
