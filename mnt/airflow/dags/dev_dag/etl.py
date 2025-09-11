import os
from typing import List
from dotenv import load_dotenv
import pandas as pd

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
import boto3
from botocore.client import Config

CSV_DIR = "/opt/airflow/include/{date}"

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
def etl():
  """
    Sensor searches for new files in the "include" folder by date.
    Files are ingested into MinIO in Parquet format, partitioned by date, and stored in the "raw" bucket.
    PySpark scripts perform transformation and validation using Great Expectations, then load the data into the "processed" bucket.
  """

  # Espera pelo diretório com arquivos
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
      folder = CSV_DIR.format(date=TODAY_DATE)
      files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
      return files

  @task # Faz upload dos CSVs para o MinIO
  def upload_file_to_minio(file_path: str):
      from python.minio import MinioUtils

      print(f"Processando: {file_path}")
      df = pd.read_csv(file_path)

      dataset_name = os.path.basename(file_path).replace(".csv", "")

      minio = MinioUtils(
        S3_CLIENT
      )

      minio.upload_df_as_parquet(df, dataset_name, bucket_name="raw")

  
  files = list_csv_files()
  wait_for_file >> files >> upload_file_to_minio.expand(file_path=files)

dag = etl()
