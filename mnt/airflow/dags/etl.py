from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor

import os
from dotenv import load_dotenv
import pandas as pd
from python.minio_uploader import MinioUploader

CSV_DIR = "/opt/airflow/include/{date_folder}"

load_dotenv()
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
def etl():
  """
    Sensor searches for new files in the "include" folder by date.
    Files are ingested into MinIO in Parquet format, partitioned by date, and stored in the "raw" bucket.
    PySpark scripts perform transformation and validation using Great Expectations, then load the data into the "processed" bucket.
  """

  # Espera pelo diretório com arquivos
  date_folder = datetime.today().strftime('%Y-%m-%d')
  wait_for_file = FileSensor(
  task_id="wait_for_file",
  filepath=f"/opt/airflow/include/{date_folder}",
  fs_conn_id="fs_default", 
  poke_interval=60,  
  timeout=60 * 60,   
  mode="poke",       
  )

  @task # Lista todos os CSVs disponíveis
  def list_csv_files():
      date_folder = datetime.today().strftime('%Y-%m-%d')
      folder = CSV_DIR.format(date_folder=date_folder)
      files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
      return files

  @task # Faz upload dos CSVs para o MinIO
  def upload_file_to_minio(file_path: str):
      print(f"Processando: {file_path}")
      df = pd.read_csv(file_path)

      dataset_name = os.path.basename(file_path).replace(".csv", "")
      today = datetime.today().strftime('%Y-%m-%d')

      uploader = MinioUploader(
          endpoint_url=ENDPOINT_URL,
          access_key=ACCESS_KEY,
          secret_key=SECRET_KEY,
          bucket_name="raw"
      )

      uploader.upload_df_as_parquet(df, dataset_name)

  
  file_list = list_csv_files()
  wait_for_file >> file_list >> upload_file_to_minio.expand(file_path=file_list)

dag = etl()
