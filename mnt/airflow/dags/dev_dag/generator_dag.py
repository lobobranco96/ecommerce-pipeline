from airflow.decorators import dag, task
from datetime import datetime, timedelta
from python.data_generator import DataGenerator

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="fake_data_generator_dag",
    description="Gera dados fake em CSV para simular dados de ecommerce",
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["fake_data", "csv", "ecommerce"],
)
def fake_data_generator_dag():

    @task()
    def generate_fake_data():
        users = 1_000
        products = 1_000
        orders = 200_000

        generator = DataGenerator()
        generator.run_all(n_users=users, n_products=products, n_orders=orders)

    generate_fake_data()

dag = fake_data_generator_dag()
