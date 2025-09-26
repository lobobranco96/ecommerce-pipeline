CREATE TABLE orders (
    order_id     VARCHAR PRIMARY KEY,
    user_id      VARCHAR NOT NULL,
    product_id   VARCHAR NOT NULL,
    quantity     INTEGER,
    total_price  NUMERIC(10,2),
    order_date   TIMESTAMP,
    status       VARCHAR,
    order_year   INTEGER,
    order_month  INTEGER,
    order_day    INTEGER
);


CREATE TABLE payments (
    payment_id      VARCHAR PRIMARY KEY,
    order_id        VARCHAR NOT NULL,
    payment_method  VARCHAR,
    amount          NUMERIC(10,2),
    paid_at         TIMESTAMP,
    paid_year       INTEGER,
    paid_month      INTEGER,
    paid_day        INTEGER
);

CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    name       VARCHAR,
    category   VARCHAR,
    price      NUMERIC(10,2),
    stock      INTEGER CHECK
);

CREATE TABLE users (
    user_id      VARCHAR PRIMARY KEY,
    name         VARCHAR,
    email        VARCHAR NOT NULL,
    signup_date  DATE,
    city         VARCHAR,
    state        VARCHAR
);

ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(user_id);
ALTER TABLE orders ADD FOREIGN KEY (product_id) REFERENCES products(product_id);
ALTER TABLE payments ADD FOREIGN KEY (order_id) REFERENCES orders(order_id);
