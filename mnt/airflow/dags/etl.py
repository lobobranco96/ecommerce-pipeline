from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import datetime, timedelta

import os
import pandas as pd
from include.minio_uploader import MinioUploader

CSV_DIR = "/opt/airflow/include/{date_folder}"

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

    @task
    def list_csv_files():
        date_folder = datetime.today().strftime('%Y-%m-%d')
        folder = CSV_DIR.format(date_folder=date_folder)
        files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
        return files

    @task
    def upload_file_to_minio(file_path: str):
        print(f"Processando: {file_path}")
        df = pd.read_csv(file_path)

        dataset_name = os.path.basename(file_path).replace(".csv", "")
        today = datetime.today().strftime('%Y-%m-%d')

        uploader = MinioUploader(
            endpoint_url="http://minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket_name="raw"
        )

        uploader.upload_df_as_parquet(df, dataset_name)

    file_list = list_csv_files()
    upload_file_to_minio.expand(file_path=file_list)

dag = ingest_csv_to_minio()
