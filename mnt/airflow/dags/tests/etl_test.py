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
    def upload_orders(files_dict: dict):
        upload(files_dict["orders"], "orders")

    @task
    def upload_payments(files_dict: dict):
        upload(files_dict["payments"], "payments")

    @task
    def upload_products(files_dict: dict):
        upload(files_dict["products"], "products")

    @task
    def upload_users(files_dict: dict):
        upload(files_dict["users"], "users")

    files_task = list_staging("{{ params.execution_date }}")

    orders_upload = upload_orders(files_task)
    payments_upload = upload_payments(files_task)
    products_upload = upload_products(files_task)
    users_upload = upload_users(files_task)

    wait_for_file >> files_task
    files_task >> [orders_upload, payments_upload, products_upload, users_upload]

    # TRANSFORM 
    today = datetime.today()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    conf = {
        "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                      "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    orders = f"s3a://{bucket}/orders/year={year}/month={month}/day={day}/orders.parquet"
    payments = f"s3a://{bucket}/payments/year={year}/month={month}/day={day}/payments.parquet"
    products = f"s3a://{bucket}/products/year={year}/month={month}/day={day}/products.parquet"
    users = f"s3a://{bucket}/users/year={year}/month={month}/day={day}/users.parquet"

    spark_orders = SparkSubmitOperator(
        task_id="transformation_orders",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", orders],
    )

    spark_payments = SparkSubmitOperator(
        task_id="transformation_payments",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", payments],
    )

    spark_products = SparkSubmitOperator(
        task_id="transformation_products",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", products],
    )

    spark_users = SparkSubmitOperator(
        task_id="transformation_users",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", users],
    )

    orders_upload >> spark_orders
    payments_upload >> spark_payments
    products_upload >> spark_products
    users_upload >> spark_users

etl_pipeline()
