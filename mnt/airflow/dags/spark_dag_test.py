import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import boto3

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["minio", "ingestion", "csv", "pyspark", "postgres"],
    default_args=default_args,
)
def test():

  @task
  def raw_file_list():
    s3_client = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY",
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    date_folder = datetime.today().strftime('%Y-%m-%d')
    response = s3_client.list_objects_v2(Bucket="raw", Prefix=date_folder)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    
    for f in files:
        logger.info(f"Arquivo encontrado: {f}")
    
    return files

  @task
  def transformation(**kwargs):

    logging.info(f"File Path: {file_path}")
    return SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/dags/spark',
    conn_id='spark_default',
    conf={
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        "spark.jars": "/opt/spark_job/jars/aws-java-sdk-bundle-1.12.262.jar,"
        "/opt/spark_job/jars/hadoop-aws-3.3.4.jar",
        "spark.hadoop.fs.s3a.endpoint": ENDPOINT_URL,
        "spark.hadoop.fs.s3a.access.key": ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": SECRET_KEY,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true"
    },
    application_args=[file_path],
    verbose=True
        ).execute(kwargs)

  file_path = raw_file_list()
  raw_file_list >> transformation.expand(file_path=file_path)

dag = test()