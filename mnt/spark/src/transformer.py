from datetime import datetime
import logging 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, col, trim, lower

logger = logging.getLogger(__name__)

class Transformer:
  def __init__(self, spark):
    self.spark = spark
    self.processed_bucket = "processed_bucket/"

  def orders(self, file_path: str):

    order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),      
    StructField("total_price", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("status", StringType(), True)
    ])
    df = self.spark.read.format("csv").schema(order_schema).option("header", True).load(file_path)

    df_transformed = df \
        .withColumn("order_year", year("order_date")) \
        .withColumn("order_month", month("order_date")) \
        .withColumn("order_day", dayofmonth("order_date")) \
        .withColumn("status", trim(lower("status"))) \
        .na.fill({"total_price": 0.0})  \
        .na.drop(subset=["user_id", "total_price"])

    return df_transformed
    # today = datetime.today()
    # year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

    # key = f"orders/year={year}/month={month}/day={day}/orders.parquet"
    # file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/processed/" + key
    # #processed_path = file_path + key
    # try:
    #   df.write \
    #       .mode("overwrite") \
    #       .parquet(file_path)
    #   logger.info(f"Data written successfully to {file_path}")
    # except Exception as e:
    #     logger.error(f"Error writing the data to {file_path}: {str(e)}")
    #     raise e

  def payments(self, file_path: str):
    payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("paid_at", TimestampType(), True)
    ])
    df = self.spark.read.format("csv").schema(payment_schema).option("header", True).load(file_path)

    df_transformed = df \
        .withColumn("paid_year", year("paid_at")) \
        .withColumn("paid_month", month("paid_at")) \
        .withColumn("paid_day", dayofmonth("paid_at")) \
        .withColumn("payment_method", lower(col("payment_method"))) \
        .filter(col("amount").isNotNull() & (col("amount") > 0))
    return df_transformed
    # today = datetime.today()
    # year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

    # key = f"payments/year={year}/month={month}/day={day}/payments.parquet"
    # file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/processed/" + key
    # #processed_path = file_path + key
    # try:
    #   df.write \
    #       .mode("overwrite") \
    #       .parquet(file_path)
    #   logger.info(f"Data written successfully to {file_path}")
    # except Exception as e:
    #     logger.error(f"Error writing the data to {file_path}: {str(e)}")
    #     raise e
  
  def products(self, file_path: str):
    product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock", IntegerType(), True)
    ])
    df = self.spark.read.format("csv").schema(product_schema).option("header", True).load(file_path)

    df_transformed = df \
        .withColumn("category", lower(trim(col("category")))) \
        .withColumn("name", trim(col("name"))) \
        .filter(col("price").isNotNull() & (col("price") > 0)) \
        .filter(col("stock").isNotNull() & (col("stock") >= 0))
    return df_transformed
    # today = datetime.today()
    # year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

    # key = f"products/year={year}/month={month}/day={day}/products.parquet"
    # file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/processed/" + key
    # #processed_path = file_path + key
    # try:
    #   df.write \
    #       .mode("overwrite") \
    #       .parquet(file_path)
    #   logger.info(f"Data written successfully to {file_path}")
    # except Exception as e:
    #     logger.error(f"Error writing the data to {file_path}: {str(e)}")
    #     raise e
  
  def users(self, file_path: str):
    user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
    ])
    df = self.spark.read.format("csv").schema(user_schema).option("header", True).load(file_path)
    df_transformed = df \
        .filter(col("user_id").isNotNull()) \
        .withColumn("name", trim(col("name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("state", trim(col("state")))

    return df_transformed
    # today = datetime.today()
    # year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

    # key = f"users/year={year}/month={month}/day={day}/users.parquet"
    # file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/processed/" + key
    # #processed_path = file_path + key
    # try:
    #   df.write \
    #       .mode("overwrite") \
    #       .parquet(file_path)
    #   logger.info(f"Data written successfully to {file_path}")
    # except Exception as e:
    #     logger.error(f"Error writing the data to {file_path}: {str(e)}")
    #     raise e
