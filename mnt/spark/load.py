import os
import sys
import logging
from utils.load_postgres import PostgresLoader
from utils.spark_session import create_spark_session
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Uso: spark-submit main.py <caminho_para_dataset_parquet>")
        sys.exit(1)

    dataset_path = sys.argv[1]
    dataset_name = dataset_path.split("/")[-1].split(".")[0]

    logger.info("Criando a sessão Spark.")
    spark = create_spark_session()

    try:
        logger.info(f"Iniciando o carregamento do dataset '{dataset_name}' localizado em '{dataset_path}'")

        load = PostgresLoader(spark)

        if hasattr(load, dataset_name):
            method = getattr(load, dataset_name)
            method(dataset_path)
            logger.info(f"Carregamento finalizado com sucesso para o dataset '{dataset_name}'")
        else:
            raise AttributeError(f"O dataset '{dataset_name}' não possui método correspondente no Loader")

    except AnalysisException as e:
        logger.error(f"Erro ao ler o Parquet de {dataset_path}: {str(e)}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        sys.exit(1)
