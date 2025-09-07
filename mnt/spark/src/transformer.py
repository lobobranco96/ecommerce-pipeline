
class Transformer:
  def __init__(self, spark):
    self.spark = spark

  def order(self):
    order_id, user_id, product_id, quantity, total_price, order_date, status
    pass

  def payments(self):
    payment_id, order_id, payment_method, amount, paid_at
    pass
  
  def products(self):
    product_id, name, category, price, stock
    pass
  
  def users(self):
    user_id, name, email, signup_date, city, state
    pass
