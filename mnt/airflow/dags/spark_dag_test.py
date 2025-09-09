import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import boto3
from botocore.client import Config

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    schedule=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["minio", "ingestion", "csv", "pyspark", "postgres"],
    default_args=default_args,
)
def test():

    @task
    def list_raw_files():
      from python.s3 import list_raw_objects

      files = list_raw_objects(S3_CLIENT)
      return files


    files = list_raw_files()

    if files:
        spark_task = SparkSubmitOperator(
            task_id="spark_submit_task",
            application="/opt/spark/main.py",
            conn_id="spark_default",
            conf={
                "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "spark.hadoop.fs.s3a.endpoint": ENDPOINT_URL,
                "spark.hadoop.fs.s3a.access.key": ACCESS_KEY,
                "spark.hadoop.fs.s3a.secret.key": SECRET_KEY,
            },
            application_args=[files], 
            verbose=True,
        )
    else:
        logger.info("Nenhum arquivo encontrado para processar. Spark task será pulada.")

dag = test()
