import sys
import logging

import pyspark
from src.transformer import Transformer
from src.spark_session import create_spark_session


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    """
    Ponto de entrada do script. Inicializa a sessão Spark e executa a transformação dos dados.
    """

    logging.info("Criando a sessão spark.")
    spark = create_spark_session()


    logging.info("Coletando o nome do dataset à ser processado e transformado.")
    dataset_path = sys.argv[0]
    dataset_name = dataset_path.split("/")[0]

    logging.info("Iniciando a transformação.")
    transformer = Transformer(spark)
    transformer.dataset_name(dataset_path)
    logging.info("Transformação finalizada.")

    logging.info("Encerrando a sessão spark.")
    spark.stop()