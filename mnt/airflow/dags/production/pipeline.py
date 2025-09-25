import os
from datetime import datetime, timedelta
import pandas as pd
from botocore.client import Config
from python.minio import MinioUtils

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log

STAGING_DIR = "/opt/airflow/include/{date}"
SPARK_PROCESSING_SCRIPT = "/opt/airflow/dags/spark/processing.py"
SPARK_LOAD_SCRIPT = "/opt/airflow/dags/spark/load.py"

def upload(file_path: str, dataset_name: str) -> str:
    """
    Realiza leitura de arquivo CSV e envia como Parquet para o bucket `raw/` no MinIO.

    Args:
        file_path (str): Caminho absoluto do arquivo CSV.
        dataset_name (str): Nome lógico do dataset (ex: 'orders').

    Returns:
        str: Caminho S3 do arquivo Parquet no MinIO.
    """
    
    logger.info(f"Lendo o diretorio: {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Carregando no bucket: raw")
    # Config MinIO
    s3_endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    minio = MinioUtils(s3_endpoint, access_key, secret_key)
    s3_path = minio.upload_df_as_parquet(df, dataset_name, bucket_name="raw")
    return s3_path  # Retorna caminho do parquet no MinIO/S3

def spark_transform_task(task_id, app_path, args):
    """
    Cria um SparkSubmitOperator configurado para executar um script Spark com argumentos.

    Args:
        task_id (str): ID da task no DAG.
        app_path (str): Caminho para o script Spark a ser executado.
        args (str): Argumento a ser passado ao script (geralmente o caminho no S3/MinIO).

    Returns:
        SparkSubmitOperator: Task configurada para execução via Spark.
    """
    return SparkSubmitOperator(
        task_id=task_id,
        application=app_path,
        conn_id="spark_default",
        verbose=True,
        application_args=[args],
    )

def spark_load_task(task_id, app_path, args):
    return SparkSubmitOperator(
        task_id=task_id,
        application=app_path,
        conn_id="spark_default",
        verbose=True,
        application_args=[args],
    )

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
    """
    Pipeline ETL para processamento de dados CSV em MinIO com Spark e carga no Postgres.

    Etapas:
    1. Aguarda arquivos CSV em uma pasta local usando FileSensor (modo reschedule).
    2. Lê os arquivos CSV e envia para o bucket `raw/` no MinIO em formato Parquet.
    3. Usa SparkSubmitOperator para transformar os dados e gravar no bucket `processed/`.
    4. Realiza carga dos dados transformados no PostgreSQL via JDBC.

    Tabelas processadas:
    - Orders
    - Payments
    - Products
    - Users

    Cada domínio é isolado via TaskGroup, permitindo falhas independentes e reprocessamento granular.
    """
    
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
        """
        Lista e valida os arquivos CSV no diretório de staging para a data informada.
    
        Args:
            date (str): Data no formato YYYY-MM-DD para montar o caminho do staging.
    
        Returns:
            dict: Mapeamento dos datasets com caminhos locais para os arquivos CSV.
    
        Raises:
            ValueError: Se não houver pelo menos 4 arquivos CSV no diretório.
        """
    
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
        """
        Realiza upload do arquivo de pedidos (orders) para o MinIO no bucket raw/.
    
        Args:
            files_dict (dict): Dicionário com os caminhos dos arquivos CSV.
    
        Returns:
            str: Caminho S3 do arquivo Parquet gerado.
        """
        return upload(files_dict["orders"], "orders")

    @task
    def raw_payments(files_dict: dict):
        """
        Realiza upload do arquivo de pedidos (payments) para o MinIO no bucket raw/.
    
        Args:
            files_dict (dict): Dicionário com os caminhos dos arquivos CSV.
    
        Returns:
            str: Caminho S3 do arquivo Parquet gerado.
        """
        return upload(files_dict["payments"], "payments")

    @task
    def raw_products(files_dict: dict):
        """
        Realiza upload do arquivo de pedidos (products) para o MinIO no bucket raw/.
    
        Args:
            files_dict (dict): Dicionário com os caminhos dos arquivos CSV.
    
        Returns:
            str: Caminho S3 do arquivo Parquet gerado.
        """
        return upload(files_dict["products"], "products")

    @task
    def raw_users(files_dict: dict):
        """
        Realiza upload do arquivo de pedidos (users) para o MinIO no bucket raw/.
    
        Args:
            files_dict (dict): Dicionário com os caminhos dos arquivos CSV.
    
        Returns:
            str: Caminho S3 do arquivo Parquet gerado.
        """
        return upload(files_dict["users"], "users")

    files_task = list_staging("{{ params.execution_date }}")

    with TaskGroup("orders") as orders_group:
        extract_orders = raw_orders(files_task)
        transform_orders = spark_transform_task("transform_orders", SPARK_PROCESSING_SCRIPT, extract_orders)
        load_orders = spark_load_task("load_orders", SPARK_LOAD_SCRIPT, extract_orders)
        
        extract_orders >> transform_orders >> load_orders

    with TaskGroup("users") as users_group:
        extract_users = raw_users(files_task)
        transform_users = spark_transform_task("transform_users", SPARK_PROCESSING_SCRIPT, extract_users)
        load_users = spark_load_task("load_users", SPARK_LOAD_SCRIPT, extract_users)
    
        extract_users >> transform_users >> load_users

    with TaskGroup("payments") as payments_group:
        extract_payments = raw_payments(files_task)
        transform_payments = spark_transform_task("transform_payments", SPARK_PROCESSING_SCRIPT, extract_payments)
        load_payments = spark_load_task("load_payments", SPARK_LOAD_SCRIPT, extract_payments)
    
        extract_payments >> transform_payments >> load_payments
        
    with TaskGroup("products") as products_group:
        extract_products = raw_products(files_task)
        transform_products = spark_transform_task("transform_products", SPARK_PROCESSING_SCRIPT, extract_products)
        load_products = spark_load_task("load_products", SPARK_LOAD_SCRIPT, extract_products)
    
        extract_products >> transform_products >> load_products

    wait_for_file >> files_task
    files_task >> [orders_group, users_group, payments_group, products_group]
    
etl_pipeline()



