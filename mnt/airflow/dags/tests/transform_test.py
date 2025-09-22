import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
)
def transform_test():
    today = datetime.today()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")


    orders = f"orders/year={year}/month={month}/day={day}/orders.parquet"
    payments = f"payments/year={year}/month={month}/day={day}/payments.parquet"
    products = f"products/year={year}/month={month}/day={day}/products.parquet"
    users = f"users/year={year}/month={month}/day={day}/users.parquet"

    conf = {
        "spark.jars": "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
                      "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }

    spark_orders = SparkSubmitOperator(
        task_id="transformation_orders",
        application="/opt/airflow/dags/spark/processingpy",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", orders],
    )

    spark_payments = SparkSubmitOperator(
        task_id="transformation_payments",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", payments],
    )

    spark_products = SparkSubmitOperator(
        task_id="transformation_products",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", products],
    )

    spark_users = SparkSubmitOperator(
        task_id="transformation_users",
        application="/opt/airflow/dags/spark/processing.py",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=["--input", users],
    )

    [spark_orders, spark_payments, spark_products, spark_users]

transform_test()
