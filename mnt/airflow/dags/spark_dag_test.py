import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from urllib.parse import unquote

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ENDPOINT_URL = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

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
        s3_client = boto3.client(
            's3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        today = datetime.today()
        today_year = today.strftime('%Y')
        today_month = today.strftime('%m')
        today_day = today.strftime('%d')

        response = s3_client.list_objects_v2(Bucket="raw", Prefix="")
        files_today = [
            obj['Key'] for obj in response.get('Contents', [])
            if f"year={today_year}/month={today_month}/day={today_day}" in obj['Key']
        ]

        for f in files_today:
            logger.info(f"Arquivo do dia de hoje encontrado: {f}")

        if not files_today:
            logger.warning("Nenhum arquivo do dia de hoje foi encontrado no bucket 'raw'.")

        return files_today[0]


    files = list_raw_files()

    if files:
        spark_task = SparkSubmitOperator(
            task_id="spark_submit_task",
            application="/opt/spark/main.py",
            conn_id="spark_default",
            conf={
                "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "spark.executor.memory": "512m",
                "spark.executor.cores": "1",
                "spark.hadoop.fs.s3a.endpoint": ENDPOINT_URL,
                "spark.hadoop.fs.s3a.access.key": ACCESS_KEY,
                "spark.hadoop.fs.s3a.secret.key": SECRET_KEY,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.path.style.access": "true",
            },
            application_args=[files], 
            verbose=True,
        )
    else:
        logger.info("Nenhum arquivo encontrado para processar. Spark task será pulada.")

dag = test()
