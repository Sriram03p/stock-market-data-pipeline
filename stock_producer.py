import json
import time
import requests
from kafka import KafkaProducer

# Alpha Vantage API Key (Get a free one at https://www.alphavantage.co/)
API_KEY = "your_alpha_vantage_api_key"
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]  # Add more stock symbols as needed

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_stock_price(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}"
    response = requests.get(url).json()
    time_series = response.get("Time Series (1min)", {})
    
    if time_series:
        latest_time = sorted(time_series.keys())[-1]
        latest_data = time_series[latest_time]
        stock_data = {
            "symbol": symbol,
            "timestamp": latest_time,
            "price": latest_data["1. open"]
        }
        return stock_data
    return None

while True:
    for symbol in STOCK_SYMBOLS:
        stock_price = fetch_stock_price(symbol)
        if stock_price:
            producer.send("stock_prices", stock_price)
            print("Sent:", stock_price)
    time.sleep(60)  # Fetch new prices every minute
