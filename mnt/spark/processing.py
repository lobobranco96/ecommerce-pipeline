import sys
import logging

from utils.transformation import Transformer
from utils.spark_session import create_spark_session
from utils.gx_validator import validate_spark_df

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    """
    Ponto de entrada do script. Inicializa a sessão Spark e executa a transformação dos dados.
    """

    logging.info("Criando a sessão spark.")
    spark = create_spark_session()

    logging.info("Coletando o nome do dataset à ser processado e transformado.")
    # O caminho do arquivo vem como argumento da DAG
    dataset_path = sys.argv[1]  
    dataset_name = dataset_path.split("/")[-1].split(".")[0]  

    logging.info(f"Dataset: {dataset_name} | Path: {dataset_path}")
    
    logging.info("Iniciando a transformação.")
    transformer = Transformer(spark, validate_spark_df)

    # Chama dinamicamente o método certo (orders, payments, etc)
    if hasattr(transformer, dataset_name):
        method = getattr(transformer, dataset_name)
        df = method(dataset_path)
        logging.info(f"Transformação finalizada para {dataset_name}")
    else:
        raise AttributeError(f"O dataset '{dataset_name}' não possui método correspondente no Transformer")

    logging.info("Encerrando a sessão spark.")