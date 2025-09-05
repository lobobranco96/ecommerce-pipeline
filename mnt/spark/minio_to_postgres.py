import logging
from pyspark.sql.utils import AnalysisException
from src.spark_session import create_spark_session

logger = logging.getLogger(__name__)