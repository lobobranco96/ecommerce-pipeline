import os
from datetime import datetime, timedelta
import pandas as pd
import boto3
from botocore.client import Config
from python.minio import MinioUtils

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log

# Config MinIO
ENDPOINT_URL = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

STAGING_DIR = "/opt/airflow/include/{date}"

def upload(file_path: str, dataset_name: str) -> str:
    logger.info(f"Lendo o diretorio: {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Carregando no bucket: raw")
    S3_CLIENT = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
    )
    MINIO = MinioUtils(S3_CLIENT)
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
    
    # LOAD POSTGRES
    conf_postgres = {
            "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
                            "/opt/spark/jars/postgresql-42.7.5.jar",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    
    load_orders = SparkSubmitOperator(
        task_id="load_orders",
        application="/opt/airflow/dags/spark/load.py",
        conn_id="spark_default",
        conf=conf_postgres,
        verbose=True,
        application_args=[orders_upload],
    )

    load_payments = SparkSubmitOperator(
        task_id="load_payments",
        application="/opt/airflow/dags/spark/load.py",
        conn_id="spark_default",
        conf=conf_postgres,
        verbose=True,
        application_args=[payments_upload],
    )

    load_products = SparkSubmitOperator(
        task_id="load_products",
        application="/opt/airflow/dags/spark/load.py",
        conn_id="spark_default",
        conf=conf_postgres,
        verbose=True,
        application_args=[products_upload],
    )

    load_users = SparkSubmitOperator(
        task_id="load_users",
        application="/opt/airflow/dags/spark/load.py",
        conn_id="spark_default",
        conf=conf_postgres,
        verbose=True,
        application_args=[users_upload],
    )

    # App 
    files_task = list_staging("{{ params.execution_date }}")

    orders_upload = raw_orders(files_task)
    payments_upload = raw_payments(files_task)
    products_upload = raw_products(files_task)
    users_upload = raw_users(files_task)

    # Listagem arquivos → uploads
    uploads = [orders_upload, payments_upload, products_upload, users_upload]
    transformation_tasks = [spark_orders, spark_payments, spark_products, spark_users]
    load_tasks = [load_orders, load_payments, load_products, load_users]

    # Sensor → listagem arquivos
    wait_for_file >> files_task

    # Listagem arquivos → uploads
    files_task >> uploads 

    # Uploads → Spark transform
    for upload, transformation_task in zip(uploads, transformation_tasks):
        upload >> transformation_task

    # Spark transform → Load
    for transformation_task, load_task in zip(transformation_tasks, load_tasks):
        transformation_task >> load_task

etl_pipeline()
