from datetime import datetime
import logging 

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
)
from pyspark.sql.functions import (
    year, month, dayofmonth, col, trim, lower, current_timestamp
)

logger = logging.getLogger(__name__)

class Transformer:
  def __init__(self, spark):
      self.spark = spark
      self.processed_bucket = "processed_bucket/"
      self.now = datetime.now()

  def orders(self, file_path: str):
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
      df_transformed.write \
          .mode("overwrite") \
          .parquet(file_processed_path)
      logger.info(f"Data written successfully to {file_path}")
    except Exception as e:
        logger.error(f"Error writing the data to {file_path}: {str(e)}")
        raise e


  def payments(self, file_path: str):
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
      df_transformed.write \
          .mode("overwrite") \
          .parquet(file_processed_path)
      logger.info(f"Data written successfully to {file_path}")
    except Exception as e:
        logger.error(f"Error writing the data to {file_path}: {str(e)}")
        raise e

  def products(self, file_path: str):
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
      df_transformed.write \
          .mode("overwrite") \
          .parquet(file_processed_path)
      logger.info(f"Data written successfully to {file_path}")
    except Exception as e:
        logger.error(f"Error writing the data to {file_path}: {str(e)}")
        raise e

  def users(self, file_path: str):
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
      df_transformed.write \
          .mode("overwrite") \
          .parquet(file_processed_path)
      logger.info(f"Data written successfully to {file_path}")
    except Exception as e:
        logger.error(f"Error writing the data to {file_path}: {str(e)}")
        raise e