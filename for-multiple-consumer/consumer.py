# consumer.py
import json
import snowflake.connector
from kafka import KafkaConsumer

# Snowflake connection
conn = snowflake.connector.connect(
    user="SRIRAM",
    password="TNMp7YBaBd9u6qJ",
    account="UXUUHAA-GE37218"
)
cur = conn.cursor()
cur.execute("USE WAREHOUSE STOCK_WAREHOUSE")
cur.execute("USE DATABASE STOCK_DB")
cur.execute("USE SCHEMA STOCK_SCHEMA")
cur.execute("""
CREATE TABLE IF NOT EXISTS stock_prices (
    symbol STRING,
    timestamp STRING,
    price FLOAT
)
""")

# Kafka consumer with 3 bootstrap servers
consumer = KafkaConsumer(
    "stock_prices",
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Consume exactly 5 messages
message_count = 0
for message in consumer:
    stock_data = message.value
    print("Received:", stock_data)

    cur.execute(f"""
        INSERT INTO stock_prices (symbol, timestamp, price)
        VALUES ('{stock_data["symbol"]}', '{stock_data["timestamp"]}', {stock_data["price"]})
    """)
    conn.commit()

    message_count += 1
    if message_count >= 5:
        break

cur.close()
conn.close()
