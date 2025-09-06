from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor

import os
from dotenv import load_dotenv
import pandas as pd
from python.minio_uploader import MinioUploader

CSV_DIR = "/opt/airflow/include/{date_folder}"

load_dotenv()

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["minio", "ingestion", "csv", "pyspark", "postgres"],
    default_args={
        "owner": "lobobranco",
        "retries": 2,
        "retry_delay": timedelta(minutes=2)
    }
)
def ingest_csv_to_minio():

    # Espera pelo diretório com arquivos
    date_folder = datetime.today().strftime('%Y-%m-%d')
    wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath=f"/opt/airflow/include/{date_folder}",
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

        endpoint_url = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")

        uploader = MinioUploader(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name="raw"
        )

        uploader.upload_df_as_parquet(df, dataset_name)

    
    file_list = list_csv_files()
    wait_for_file >> file_list >> upload_file_to_minio.expand(file_path=file_list)

dag = ingest_csv_to_minio()
