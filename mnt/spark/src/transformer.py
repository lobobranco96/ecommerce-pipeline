from datetime import datetime
import logging 
import json
import os
from great_expectations.dataset import SparkDFDataset

from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DateType, TimestampType
)
from pyspark.sql.functions import (
    year, month, dayofmonth, col, trim, lower, current_timestamp
)

logger = logging.getLogger(__name__)

class Transformer:
    """
    Classe para transformação de dados de e-commerce.
    Cada método lê arquivos Parquet da camada 'raw', aplica cast de tipos,
    limpeza de dados e escreve a saída na camada 'processed'.
    """

    def __init__(self, spark):
        """
        Inicializa o Transformer com uma SparkSession.

        Args:
            spark (SparkSession): Sessão do PySpark.
        """
        self.spark = spark
        self.processed_bucket = "processed_bucket/"
        self.now = datetime.now()

    def validate_with_json(self, parquet_path: str, json_path: str):
        """
        Valida um dataset Parquet salvo no S3 usando um JSON de expectativas.
        O resultado da validação é salvo no mesmo diretório do parquet.

        Args:
            parquet_path (str): Caminho do arquivo parquet já gravado.
            json_path (str): Caminho do JSON com as expectativas.
        """
        # Lê parquet salvo
        df = self.spark.read.parquet(parquet_path)

        # Converte para GE dataset
        spark_df = SparkDFDataset(df)

        # Carrega regras do JSON
        with open(json_path, "r") as f:
            expectations = json.load(f).get("expectations", [])

        validation_results = []
        for expectation in expectations:
            expectation_type = expectation["expectation_type"]
            kwargs = expectation.get("kwargs", {})
            result = getattr(spark_df, expectation_type)(**kwargs)
            validation_results.append({
                "expectation_type": expectation_type,
                "kwargs": kwargs,
                "success": result["success"]
            })

        # Caminho do arquivo de validação
        validation_path = parquet_path.replace(".parquet", "_validation.json")

        # Salva JSON de resultado no mesmo bucket
        results_df = self.spark.createDataFrame(validation_results)
        results_df.coalesce(1).write.mode("overwrite").json(validation_path)

        return validation_path

    def orders(self, file_path: str):
        """
        Transforma o dataset de pedidos (orders).

        Operações:
        - Lê o arquivo Parquet e infere o schema.
        - Converte colunas para tipos corretos.
        - Remove duplicatas.
        - Adiciona colunas de ano, mês e dia do pedido.
        - Limpa dados inválidos (quantidade <= 0, total_price <= 0, datas no futuro).
        - Normaliza a coluna 'status'.
        - Escreve o resultado na pasta 'processed'.

        Args:
            file_path (str): Caminho do arquivo Parquet na camada 'raw'.

        Raises:
            Exception: Se ocorrer erro ao escrever o Parquet.
        """
        df = self.spark.read.format("parquet").load(file_path)

        df_casted = (
            df.withColumn("order_id", col("order_id").cast(StringType()))
              .withColumn("user_id", col("user_id").cast(StringType()))
              .withColumn("product_id", col("product_id").cast(StringType()))
              .withColumn("quantity", col("quantity").cast(DoubleType()))
              .withColumn("total_price", col("total_price").cast(DoubleType()))
              .withColumn("order_date", col("order_date").cast(TimestampType()))
              .withColumn("status", col("status").cast(StringType()))
        )

        df_transformed = (
            df_casted.dropDuplicates()
                .withColumn("order_year", year("order_date"))
                .withColumn("order_month", month("order_date"))
                .withColumn("order_day", dayofmonth("order_date"))
                .withColumn("status", trim(lower("status")))
                .na.drop(subset=["user_id", "quantity", "total_price"]) 
                .filter(col("quantity") > 0)                            
                .filter(col("total_price") > 0)                      
                .filter(col("order_date") <= current_timestamp())
        )

        file_processed_path = file_path.replace("raw", "processed")
        try:
            df_transformed.write.mode("overwrite").parquet(file_processed_path)
            logger.info(f"Data written successfully to {file_processed_path}")
        except Exception as e:
            logger.error(f"Error writing the data to {file_processed_path}: {str(e)}")
            raise e

        """ Gera a validação apos a gravação do arquivo no miniO com Great Expectations"""
        expectation_json = "/opt/great_expectations/gx/expectations/orders_expectations.json"

        validation_file = self.validate_with_json(
            parquet_path=file_processed_path,
            json_path=expectation_json
        )
        logger.info(f"Validation results saved at {validation_file}")

    def payments(self, file_path: str):
        """
        Transforma o dataset de pagamentos (payments).

        Operações:
        - Lê o arquivo Parquet e infere o schema.
        - Converte colunas para tipos corretos.
        - Remove duplicatas.
        - Adiciona colunas de ano, mês e dia do pagamento.
        - Normaliza a coluna 'payment_method'.
        - Limpa dados inválidos (amount <= 0, paid_at nulo ou no futuro).
        - Escreve o resultado na pasta 'processed'.

        Args:
            file_path (str): Caminho do arquivo Parquet na camada 'raw'.

        Raises:
            Exception: Se ocorrer erro ao escrever o Parquet.
        """
        df = self.spark.read.format("parquet").load(file_path)

        df_casted = (
            df.withColumn("payment_id", col("payment_id").cast(StringType()))
              .withColumn("order_id", col("order_id").cast(StringType()))
              .withColumn("payment_method", col("payment_method").cast(StringType()))
              .withColumn("amount", col("amount").cast(DoubleType()))
              .withColumn("paid_at", col("paid_at").cast(TimestampType()))
        )

        df_transformed = (
            df_casted.dropDuplicates()
                .withColumn("paid_year", year("paid_at"))
                .withColumn("paid_month", month("paid_at"))
                .withColumn("paid_day", dayofmonth("paid_at"))
                .withColumn("payment_method", lower(col("payment_method")))
                .filter(col("amount").isNotNull() & (col("amount") > 0))  
                .filter(col("paid_at").isNotNull() & (col("paid_at") <= current_timestamp()))
        )

        file_processed_path = file_path.replace("raw", "processed")
        try:
            df_transformed.write.mode("overwrite").parquet(file_processed_path)
            logger.info(f"Data written successfully to {file_processed_path}")
        except Exception as e:
            logger.error(f"Error writing the data to {file_processed_path}: {str(e)}")
            raise e

        """ Gera a validação apos a gravação do arquivo no miniO com Great Expectations"""
        expectation_json = "/opt/great_expectations/gx/expectations/payments_expectations.json"

        validation_file = self.validate_with_json(
            parquet_path=file_processed_path,
            json_path=expectation_json
        )
        logger.info(f"Validation results saved at {validation_file}")

    def products(self, file_path: str):
        """
        Transforma o dataset de produtos (products).

        Operações:
        - Lê o arquivo Parquet e infere o schema.
        - Converte colunas para tipos corretos.
        - Remove duplicatas.
        - Normaliza colunas 'name' e 'category'.
        - Filtra produtos com preço <= 0 ou estoque negativo.
        - Escreve o resultado na pasta 'processed'.

        Args:
            file_path (str): Caminho do arquivo Parquet na camada 'raw'.

        Raises:
            Exception: Se ocorrer erro ao escrever o Parquet.
        """
        df = self.spark.read.format("parquet").load(file_path)

        df_casted = (
            df.withColumn("product_id", col("product_id").cast(StringType()))
              .withColumn("name", col("name").cast(StringType()))
              .withColumn("category", col("category").cast(StringType()))
              .withColumn("price", col("price").cast(DoubleType()))
              .withColumn("stock", col("stock").cast(IntegerType()))
        )

        df_transformed = (
            df_casted.dropDuplicates()
                .withColumn("category", lower(trim(col("category"))))
                .withColumn("name", trim(col("name")))
                .filter(col("price").isNotNull() & (col("price") > 0))  
                .filter(col("stock").isNotNull() & (col("stock") >= 0))
        )

        file_processed_path = file_path.replace("raw", "processed")
        try:
            df_transformed.write.mode("overwrite").parquet(file_processed_path)
            logger.info(f"Data written successfully to {file_processed_path}")
        except Exception as e:
            logger.error(f"Error writing the data to {file_processed_path}: {str(e)}")
            raise e

        """ Gera a validação apos a gravação do arquivo no miniO com Great Expectations"""
        expectation_json = "/opt/great_expectations/gx/expectations/products_expectations.json"

        validation_file = self.validate_with_json(
            parquet_path=file_processed_path,
            json_path=expectation_json
        )
        logger.info(f"Validation results saved at {validation_file}")

    def users(self, file_path: str):
        """
        Transforma o dataset de usuários (users).

        Operações:
        - Lê o arquivo Parquet e infere o schema.
        - Converte colunas para tipos corretos.
        - Remove duplicatas.
        - Limpa dados nulos nas colunas 'user_id' e 'email'.
        - Normaliza colunas 'name', 'email', 'city' e 'state'.
        - Escreve o resultado na pasta 'processed'.

        Args:
            file_path (str): Caminho do arquivo Parquet na camada 'raw'.

        Raises:
            Exception: Se ocorrer erro ao escrever o Parquet.
        """
        df = self.spark.read.format("parquet").load(file_path)

        df_casted = (
            df.withColumn("user_id", col("user_id").cast(StringType()))
              .withColumn("name", col("name").cast(StringType()))
              .withColumn("email", col("email").cast(StringType()))
              .withColumn("signup_date", col("signup_date").cast(DateType()))
              .withColumn("city", col("city").cast(StringType()))
              .withColumn("state", col("state").cast(StringType()))
        )

        df_transformed = (
            df_casted.dropDuplicates()
                .filter(col("user_id").isNotNull())
                .filter(col("email").isNotNull())
                .withColumn("name", trim(col("name")))
                .withColumn("email", lower(trim(col("email"))))
                .withColumn("city", trim(col("city")))
                .withColumn("state", trim(col("state")))
        )

        file_processed_path = file_path.replace("raw", "processed")
        try:
            df_transformed.write.mode("overwrite").parquet(file_processed_path)
            logger.info(f"Data written successfully to {file_processed_path}")
        except Exception as e:
            logger.error(f"Error writing the data to {file_processed_path}: {str(e)}")
            raise e

        """ Gera a validação apos a gravação do arquivo no miniO com Great Expectations"""
        expectation_json = "/opt/great_expectations/gx/expectations/users_expectations.json"

        validation_file = self.validate_with_json(
            parquet_path=file_processed_path,
            json_path=expectation_json
        )
        logger.info(f"Validation results saved at {validation_file}")
