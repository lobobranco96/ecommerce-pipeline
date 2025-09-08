import logging
import pyspark
from pyspark.sql import SparkSession

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session():
    """
    Cria e configura uma sessão do Apache Spark.
    
    Returns:
        SparkSession: Sessão Spark configurada.
    
    Raises:
        Exception: Se houver falha na criação da sessão.
    """
    try:
        logger.info("Iniciando a configuração da Spark Session")
        
        conf = (
            pyspark.SparkConf()
            .set("spark.master", "spark://spark-master:7077")
            )
        
        spark = SparkSession.builder \
            .appName("Minio Integration with PySpark") \
            .config(conf=conf) \
            .getOrCreate()
        
        logger.info("Spark Session criada com sucesso")
        return spark
    
    except Exception as e:
        logger.error("Erro ao criar a Spark Session", exc_info=True)
        raise
