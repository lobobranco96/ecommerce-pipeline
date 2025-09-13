import os

from IPython.utils import strdispatch

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

class PostgresLoader:
    def __init__(self, spark):
        self.spark = spark

    def postgres_settings(self):
        postgres_url = f"jdbc:postgresql://dwh_postgres:5432/{POSTGRES_DB}"
        postgres_properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "currentSchema": "ecommerce"
        }
        return postgres_url, postgres_properties

    def load_table(self, file_path: str, table_name: str):
        url, postgres_properties = self.postgres_settings()

        df = self.spark.read.parquet(file_path)

        return df.write.jdbc(
            url=url,
            table=table_name,
            mode="append",
            properties=postgres_properties,
        )

    def orders(self, file_path: str):
        return self.load_table(file_path, "ecommerce.orders")

    def payments(self, file_path: str):
        return self.load_table(file_path, "ecommerce.payments")

    def products(self, file_path: str):
        return self.load_table(file_path, "ecommerce.products")

    def users(self, file_path: str):
        return self.load_table(file_path, "ecommerce.users")
