import os
from typing import List
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
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
TODAY_DATE= datetime.today().strftime('%Y-%m-%d')
STADING_DIR = "/opt/airflow/include/{date}"

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
@dag(
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["etl", "minio", "ingestion", "csv", "pyspark", "postgres"],
    default_args=default_args,
)
def ecommerce_etl():

  # TaskGroup de Extração
  with TaskGroup("extract", tooltip="Extração e upload CSV -> MinIO") as extract_group:
    """
      Sensor searches for new files in the "include" folder by date.
      Files are ingested into MinIO in Parquet format, partitioned by date, and stored in the "raw" bucket.
      PySpark scripts perform transformation and validation using Great Expectations, then load the data into the "processed" bucket.
    """

    wait_for_file = FileSensor(
      task_id="wait_for_file",
      filepath=f"/opt/airflow/include/{TODAY_DATE}",
      fs_conn_id="fs_default", 
      poke_interval=60,  
      timeout=60 * 60,   
      mode="poke",       
      )

    @task # Lista todos os CSVs disponíveis
    def list_csv_files() -> List[str]:
        folder = STADING_DIR.format(date=TODAY_DATE)
        files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
        return files

    @task # Faz upload dos CSVs para o MinIO
    def upload_file_to_minio(file_path: str):
        from python.minio_uploader import MinioUploader

        logger.info(f"Processando: {file_path}")
        df = pd.read_csv(file_path)

        dataset_name = os.path.basename(file_path).replace(".csv", "")

        uploader = MinioUploader(
          S3_CLIENT,
          bucket_name="raw"
        )

        uploader.upload_df_as_parquet(df, dataset_name)


    sensor = wait_for_file
    files = list_csv_files()
    upload_file_to_minio.expand(file_path=files)

  # TaskGroup de Transformação
  with TaskGroup("transform", tooltip="Transformação PySpark e carga processed") as transform_group:

      @task
      def list_raw_files():
          """Lista todos os arquivos Parquet disponíveis no bucket 'raw' do MinIO."""
          from python.s3 import list_raw_objects
          files = list_raw_objects(S3_CLIENT)
          logger.info(f"Arquivos encontrados no bucket raw: {files}")
          return files

      @task.branch
      def check_files_exist(files):
          """
          Branching para decidir se o SparkSubmit deve rodar.
          Retorna o task_id de SparkSubmit se arquivos existirem,
          caso contrário, retorna o task_id do skip.
          """
          if files:
              logger.info("Arquivos encontrados. SparkSubmit será executado.")
              return "transform.spark_submit_task"
          else:
              logger.info("Nenhum arquivo encontrado. SparkSubmit será pulado.")
              return "transform.skip_spark"

      @task
      def skip_spark():
          """Task de placeholder quando não há arquivos para processar."""
          logger.info("SparkSubmit não será executado, pois não há arquivos para processar.")

      # Não chamar diretamente as funções
      files = list_raw_files()
      branch = check_files_exist(files)

      # SparkSubmitOperator dinâmico
      spark_task = SparkSubmitOperator.partial(
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
          verbose=True,
      ).expand(
          application_args=[[f] for f in files]  # cada arquivo em uma lista
      )

      # Definindo dependências
      branch >> [spark_task, skip_spark()]
      
  extract_group >> transform_group

dag = ecommerce_etl()