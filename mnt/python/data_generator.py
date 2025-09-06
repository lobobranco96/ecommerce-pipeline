import pandas as pd
from faker import Faker
from datetime import datetime
import uuid
import os
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

class DataGenerator:
    def __init__(self):
        timestamp = datetime.today().strftime('%Y-%m-%d')
        self.output_dir = os.path.join("/opt/airflow/include", timestamp)
        os.makedirs(self.output_dir, exist_ok=True)

        self.fake = Faker('pt_BR')
        logging.info(f"Diretório de saída: {self.output_dir}")

    def users(self, quantity):
        logging.info(f"Gerando {quantity} usuários...")
        users = []
        for _ in range(quantity):
            users.append({
                "user_id": str(uuid.uuid4()),
                "name": self.fake.name(),
                "email": self.fake.email(),
                "signup_date": self.fake.date_between(start_date='-2y', end_date='today').isoformat(),
                "city": self.fake.city(),
                "state": self.fake.estado_sigla()
            })
        df = pd.DataFrame(users)

        df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
        df.loc[df.sample(frac=0.05).index, 'email'] = None
        df.loc[df.sample(frac=0.02).index, 'user_id'] = df['user_id'].sample(frac=0.02).apply(lambda _: random.randint(1000, 9999))

        file_path = os.path.join(self.output_dir, "users.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Arquivo 'users.csv' salvo com {len(df)} registros.")
        return df

    def products(self, quantity):
        logging.info(f"Gerando {quantity} produtos...")
        categorias = {
            'eletrônicos': [
                ("Smartphone", 1000, 5000),
                ("Notebook", 2000, 7000),
                ("Fone Bluetooth", 100, 800),
                ("Smart TV", 1200, 5000),
                ("Caixa de Som", 150, 1000)
            ],
            'moda': [
                ("Camiseta", 40, 120),
                ("Calça Jeans", 80, 250),
                ("Tênis", 150, 600),
                ("Jaqueta", 100, 400),
                ("Boné", 30, 80)
            ],
            'livros': [
                ("Romance", 25, 80),
                ("Livro Técnico", 60, 180),
                ("Infantil", 20, 60),
                ("Autoajuda", 30, 100),
                ("História", 40, 120)
            ],
            'casa': [
                ("Panela", 70, 300),
                ("Cadeira", 120, 600),
                ("Lâmpada", 15, 70),
                ("Sofá", 1000, 3000),
                ("Liquidificador", 100, 400)
            ],
            'esportes': [
                ("Bola", 60, 200),
                ("Bicicleta", 800, 2500),
                ("Tênis de Corrida", 200, 600),
                ("Mochila", 100, 300),
                ("Skate", 250, 800)
            ]
        }

        products = []
        for _ in range(quantity):
            categoria = random.choice(list(categorias.keys()))
            nome_produto, preco_min, preco_max = random.choice(categorias[categoria])

            products.append({
                "product_id": str(uuid.uuid4()),
                "name": nome_produto + f" {random.randint(100, 999)}",
                "category": categoria,
                "price": round(random.uniform(preco_min, preco_max), 2),
                "stock": random.randint(0, 150)
            })

        df = pd.DataFrame(products)

        df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
        df.loc[df.sample(frac=0.05).index, 'price'] = None
        df.loc[df.sample(frac=0.02).index, 'price'] *= -1

        file_path = os.path.join(self.output_dir, "products.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Arquivo 'products.csv' salvo com {len(df)} registros.")
        return df

    def orders(self, quantity, users=None, products=None):
        logging.info(f"Gerando {quantity} pedidos...")
        orders = []
        for _ in range(quantity):
            user = random.choice(users)
            product = random.choice(products)
            quantity_val = random.randint(1, 5)
            order_dt = self.fake.date_time_between(start_date='-6M', end_date='now')
            order_dt_str = order_dt.strftime('%Y-%m-%dT%H:%M:%S')

            orders.append({
                "order_id": str(uuid.uuid4()),
                "user_id": user['user_id'],
                "product_id": product['product_id'],
                "quantity": quantity_val,
                "total_price": round(product['price'] * quantity_val if product['price'] else 0, 2),
                "order_date": order_dt_str,
                "status": random.choice(["aprovado", "pendente", "cancelado"])
            })

        df = pd.DataFrame(orders)

        df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
        df.loc[df.sample(frac=0.03).index, 'quantity'] = 0
        df.loc[df.sample(frac=0.05).index, 'quantity'] = None
        df.loc[df.sample(frac=0.02).index, 'order_date'] = "2099-01-01T00:00:00"
        df.loc[df.sample(frac=0.03).index, 'user_id'] = df['user_id'].sample(frac=0.03).apply(lambda _: random.randint(1000, 9999))

        file_path = os.path.join(self.output_dir, "orders.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Arquivo 'orders.csv' salvo com {len(df)} registros.")
        return df

    def payments(self, orders_df):
        logging.info(f"Gerando pagamentos para {len(orders_df)} pedidos...")
        payments = []
        for _, order in orders_df.iterrows():
            order_date = pd.to_datetime(order["order_date"], errors='coerce')
            paid_at = self.fake.date_time_between(start_date=order_date)
            paid_at_str = paid_at.strftime('%Y-%m-%dT%H:%M:%S')

            payments.append({
                "payment_id": str(uuid.uuid4()),
                "order_id": order["order_id"],
                "payment_method": random.choice(["pix", "boleto", "cartao_credito", "cartao_debito"]),
                "amount": order["total_price"],
                "paid_at": paid_at_str
            })

        df = pd.DataFrame(payments)

        df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)
        df.loc[df.sample(frac=0.03).index, 'amount'] = -1
        df.loc[df.sample(frac=0.02).index, 'paid_at'] = None

        file_path = os.path.join(self.output_dir, "payments.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Arquivo 'payments.csv' salvo com {len(df)} registros.")
        return df

    def run_all(self, n_users=100, n_products=50, n_orders=200):
        logging.info("Iniciando o fake data generator")

        users_df = self.users(n_users)
        users_dict = pd.read_csv(os.path.join(self.output_dir, "users.csv")).to_dict('records')

        products_df = self.products(n_products)
        products_dict = products_df.to_dict('records')

        orders_df = self.orders(n_orders, users_dict, products_dict)

        self.payments(orders_df)

        logging.info(f"Todos os dados foram gerados com sucesso em: {self.output_dir}")
