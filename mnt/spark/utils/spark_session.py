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
          .set("spark.executor.memory", "512m")
          .set("spark.executor.cores", "1")
          .set("spark.driver.memory", "512m")
          .set("spark.executor.instances", "1")
          .set("spark.sql.shuffle.partitions", "8")
        #  .set("spark.memory.offHeap.enabled", "true")
        #  .set("spark.memory.offHeap.size", "64m")
          .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .set("spark.hadoop.fs.s3a.path.style.access", "true")
          
      )
        
        spark = SparkSession.builder \
            .appName("Minio Integration with PySpark") \
            .config(conf=conf) \
            .getOrCreate()

        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
        spark.conf.set("spark.sql.shuffle.partitions", "1")
        
        logger.info("Spark Session criada com sucesso")
        return spark
    
    except Exception as e:
        logger.error("Erro ao criar a Spark Session", exc_info=True)
        raise
