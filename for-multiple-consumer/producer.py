# producer.py
import json
import requests
from kafka import KafkaProducer

API_KEY = "FQAR79OPRXS4F1U6"
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

# Kafka producer with 3 bootstrap servers
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_stock_price(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}"
    response = requests.get(url).json()
    time_series = response.get("Time Series (1min)", {})
    
    if time_series:
        latest_time = sorted(time_series.keys())[-1]
        latest_data = time_series[latest_time]
        return {
            "symbol": symbol,
            "timestamp": latest_time,
            "price": latest_data["1. open"]
        }

for symbol in STOCK_SYMBOLS:
    stock_price = fetch_stock_price(symbol)
    if stock_price:
        producer.send("stock_prices", stock_price)
        print("Sent:", stock_price)

producer.flush()
