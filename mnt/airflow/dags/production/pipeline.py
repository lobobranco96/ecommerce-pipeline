import os
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

logger = LoggingMixin().log

# Config MinIO
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
    s3_path = MINIO.upload_df_as_parquet(df, dataset_name, bucket_name="raw")
    return s3_path  # Retorna caminho do parquet no MinIO/S3

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
def etl_pipeline():

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/include/{{ params.execution_date }}",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    # EXTRACT
    @task
    def list_staging(date: str) -> dict:
        folder = STAGING_DIR.format(date=date)
        files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
        logger.info(f"Arquivos CSV encontrados: {files}")
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
    def raw_orders(files_dict: dict):
        return upload(files_dict["orders"], "orders")

    @task
    def raw_payments(files_dict: dict):
        return upload(files_dict["payments"], "payments")

    @task
    def raw_products(files_dict: dict):
        return upload(files_dict["products"], "products")

    @task
    def raw_users(files_dict: dict):
        return upload(files_dict["users"], "users")

    files_task = list_staging("{{ params.execution_date }}")

    orders_upload = raw_orders(files_task)
    payments_upload = raw_payments(files_task)
    products_upload = raw_products(files_task)
    users_upload = raw_users(files_task)

    wait_for_file >> files_task
    files_task >> [orders_upload, payments_upload, products_upload, users_upload]

    # TRANSFORM 
    conf = {
        "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                      "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    spark_orders = SparkSubmitOperator(
        task_id="processed_orders",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[orders_upload],
    )

    spark_payments = SparkSubmitOperator(
        task_id="processed_payments",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[payments_upload],
    )

    spark_products = SparkSubmitOperator(
        task_id="processed_products",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[products_upload],
    )

    spark_users = SparkSubmitOperator(
        task_id="processed_users",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[users_upload],
    )

    orders_upload >> spark_orders
    payments_upload >> spark_payments
    products_upload >> spark_products
    users_upload >> spark_users

etl_pipeline()
