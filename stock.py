import streamlit as st
import pandas as pd
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime
from textblob import TextBlob
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import ollama

# ------------------- CONFIG -------------------
@st.cache_resource
def get_mongodb_client():
    # Replace with your MongoDB connection string
    client = MongoClient('mongodb://localhost:27017/')
    return client

def get_stock_data_from_mongodb():
    """
    Fetch stock data from MongoDB and normalize column names
    """
    client = get_mongodb_client()
    db = client['stock_data']  # Your database name
    collection = db['prices']  # Your collection name
    
    # Fetch data and convert to DataFrame
    cursor = collection.find().sort("TIMESTAMP", -1).limit(100)
    data = list(cursor)
    
    if data:
        df = pd.DataFrame(data)
        # Remove MongoDB's _id field if not needed
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        # Normalize column names to lowercase for consistency
        df.columns = df.columns.str.lower()
        
        # Ensure we have the expected columns
        expected_columns = ['symbol', 'timestamp', 'price']
        for col in expected_columns:
            if col not in df.columns:
                st.error(f"Missing expected column: {col}")
                st.write("Available columns:", df.columns.tolist())
                return pd.DataFrame()
        
        return df
    else:
        return pd.DataFrame()

ALPHA_VANTAGE_API_KEY = 'B5WJBWUZC8XHRVOF'
TWELVE_DATA_API_KEY = '8a97b0338c804639931f90bff73311cc'

# ------------------- LIVE STOCK FEED -------------------
# ------------------- LIVE STOCK FEED -------------------
def debug_mongodb_data():
    """
    Debug function to see what's actually in MongoDB
    """
    client = get_mongodb_client()
    db = client['stock_data']
    collection = db['prices']
    
    # Get a sample document
    sample = collection.find_one()
    if sample:
        st.write("Sample document from MongoDB:")
        st.json(sample)
        
        # Get all documents and show structure
        all_docs = list(collection.find().limit(5))
        df = pd.DataFrame(all_docs)
        st.write("DataFrame structure:")
        st.write("Columns:", df.columns.tolist())
        st.write("Data types:", df.dtypes)
        st.dataframe(df.head())
    else:
        st.error("No documents found in MongoDB collection")

def show_live_feed():
    # Add debug option
    # if st.checkbox("Debug Mode - Show MongoDB Data Structure"):
    #     debug_mongodb_data()
    #     st.divider()
    
    df = get_stock_data_from_mongodb()
    
    if df.empty:
        st.warning("No data found in MongoDB. Please ensure your stock data is loaded.")
        return
    
    st.subheader("🔴 Live Stock Prices")
    st.dataframe(df)

    # Check if we have the required columns
    if "symbol" in df.columns and "timestamp" in df.columns and "price" in df.columns:
        symbol = st.selectbox("Select a stock symbol", df["symbol"].unique())
        filtered = df[df["symbol"] == symbol].sort_values("timestamp")
        
        # Create line chart
        if not filtered.empty:
            st.line_chart(filtered.set_index("timestamp")["price"])
        else:
            st.warning(f"No data found for symbol: {symbol}")
    else:
        st.error("Required columns (symbol, timestamp, price) not found in data")
        st.write("Available columns:", df.columns.tolist())

# ------------------- STOCK INSIGHTS & FORECAST -------------------
def get_stock_news(symbol):
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='fullview-news-outer')
        rows = table.find_all('tr')[:10]
        headlines = [row.find_all('td')[1].text.strip() for row in rows]
        return pd.DataFrame({'Headlines': headlines})
    except Exception as e:
        st.warning(f"News retrieval failed: {e}")
        return pd.DataFrame()

def analyze_sentiment(text):
    text = ' '.join(text)
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity < 0:
        return "Negative"
    else:
        return "Neutral"

def get_stock_sentiment(df):
    df['Sentiment'] = df['Headlines'].apply(analyze_sentiment)
    pos = (df['Sentiment'] == 'Positive').sum()
    neg = (df['Sentiment'] == 'Negative').sum()
    if pos > neg:
        return 'green', "📈 The reviews of the stock are looking good!"
    elif neg > pos:
        return 'red', "📉 The reviews of the stock are looking bad!"
    else:
        return 'orange', "➖ The reviews of the stock are neutral."

def get_stock_data(symbol):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=100&apikey={TWELVE_DATA_API_KEY}"
        response = requests.get(url)
        data = response.json()
        if 'values' not in data:
            st.error(f"API error: {data.get('message', 'Unknown error')}")
            return pd.DataFrame()
        df = pd.DataFrame(data['values'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df = df.rename(columns={'close': 'Close'})
        df['Close'] = df['Close'].astype(float)
        df.sort_index(inplace=True)
        return df[['Close']]
    except Exception as e:
        st.error(f"Error fetching stock data: {e}")
        return pd.DataFrame()

def ets_demand_forecast(stock_data, forecast_period):
    model = ExponentialSmoothing(stock_data['Close'], trend='add', seasonal='add', seasonal_periods=30)
    fit = model.fit()
    forecast = fit.forecast(steps=forecast_period)
    return forecast

def plot_demand_forecast(stock_data, forecast):
    fig, ax = plt.subplots()
    ax.plot(stock_data['Close'], label='Historical', color='blue')
    ax.plot(pd.date_range(start=stock_data.index[-1], periods=len(forecast)+1, freq='B')[1:], forecast, label='Forecast', color='red')
    ax.set_title("🔮 Stock Price Forecast")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (USD)")
    ax.legend()
    st.pyplot(fig)

def show_stock_insights():
    stock_symbol = st.text_input("Enter Stock Symbol (e.g., AAPL, MSFT)").upper()

    if st.button("Analyze") and stock_symbol:
        st.subheader("📰 News Headlines")
        news_df = get_stock_news(stock_symbol)
        if not news_df.empty:
            st.dataframe(news_df)
            color, message = get_stock_sentiment(news_df)
            st.markdown(f'<p style="color:{color}"><b>{message}</b></p>', unsafe_allow_html=True)

        st.subheader("📊 Historical Stock Data")
        stock_data = get_stock_data(stock_symbol)
        if not stock_data.empty:
            st.line_chart(stock_data['Close'])

            st.subheader("🔮 Forecast Stock Price")
            forecast_days = st.slider("Forecast Period (days)", 10, 180, 60)
            forecast = ets_demand_forecast(stock_data, forecast_days)
            plot_demand_forecast(stock_data, forecast)

# ------------------- CHATBOT USING OLLAMA -------------------
def show_chatbot():
    st.subheader("💬 AI Chatbot (Ask your doubts here)")
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "system", "content": "You are a helpful assistant."}
        ]

    for msg in st.session_state.messages[1:]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask a question...")
    if user_input:
        st.chat_message("user").markdown(user_input)
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.chat_message("assistant"):
            response = ollama.chat(
                model="phi",
                messages=st.session_state.messages
            )
            reply = response['message']['content']
            st.markdown(reply)

        st.session_state.messages.append({"role": "assistant", "content": reply})

# ------------------- MAIN APP -------------------
def main():
    st.set_page_config(layout="wide", page_title="📊 Stock + Chatbot Dashboard")
    st.title("📈 Real-Time Stock Market + 🤖 AI Assistant")

    tab1, tab2, tab3 = st.tabs(["📡 Live Market Feed", "📊 Insights & Forecast", "🤖 AI Chatbot"])
    with tab1:
        show_live_feed()
    with tab2:
        show_stock_insights()
    with tab3:
        show_chatbot()

if __name__ == "__main__":
    main()