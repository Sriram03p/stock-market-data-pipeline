import json
import snowflake.connector
from kafka import KafkaConsumer

# Snowflake connection details
SNOWFLAKE_ACCOUNT = "UXUUHAA-GE37218"
SNOWFLAKE_USER = "SRIRAM"
SNOWFLAKE_PASSWORD = "TNMp7YBaBd9u6qJ"
SNOWFLAKE_DATABASE = "STOCK_DB"
SNOWFLAKE_SCHEMA = "STOCK_SCHEMA"
SNOWFLAKE_WAREHOUSE = "STOCK_WAREHOUSE"

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT
)

cur = conn.cursor()
cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

# Ensure table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS stock_prices (
    symbol STRING,
    timestamp STRING,
    price FLOAT
)
""")

# Kafka Consumer setup
consumer = KafkaConsumer(
    "stock_prices",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for message in consumer:
    stock_data = message.value
    print("Received:", stock_data)

    query = f"""
    INSERT INTO stock_prices (symbol, timestamp, price)
    VALUES ('{stock_data["symbol"]}', '{stock_data["timestamp"]}', {stock_data["price"]})
    """
    cur.execute(query)
    conn.commit()
