import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime
from textblob import TextBlob
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import ollama

# ------------------- CONFIG -------------------
@st.cache_resource
def get_mongodb_client():
    client = MongoClient("mongodb://localhost:27017/")  # Update if remote
    return client

# ------------------- FETCH DATA FROM MONGODB -------------------
def get_stock_data_from_mongodb(collection_name="stock_features", limit=200):
    client = get_mongodb_client()
    db = client["stock_data"]
    collection = db[collection_name]

    cursor = collection.find().sort("TIMESTAMP", -1).limit(limit)
    data = list(cursor)

    if data:
        df = pd.DataFrame(data)
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        df.columns = df.columns.str.lower()

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df
    return pd.DataFrame()

# ------------------- THEME HANDLER -------------------
def set_theme(dark_mode: bool):
    if dark_mode:
        st.markdown(
            """
            <style>
            body, .stApp { background-color: #1E1E1E; color: #E0E0E0; }
            </style>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <style>
            body, .stApp { background-color: #FFFFFF; color: #000000; }
            </style>
            """,
            unsafe_allow_html=True,
        )

# ------------------- KPI CARDS -------------------
def show_kpis(df, symbol):
    latest = df[df["symbol"] == symbol].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("📈 Last Price", f"${latest['price']:.2f}")
    col2.metric("🔄 Daily Change", f"{latest.get('daily_change', 0):.2f}")
    col3.metric("📊 Daily Return %", f"{latest.get('daily_return_percent', 0):.2f}%")

    col4, col5 = st.columns(2)
    col4.metric("📉 7D Moving Avg", f"${latest.get('moving_avg_7d', 0):.2f}")
    col5.metric("💰 Cumulative Return", f"{latest.get('cumulative_return', 0):.2f}%")

# ------------------- CHARTS -------------------
def show_charts(df, symbol):
    filtered = df[df["symbol"] == symbol]

    st.subheader(f"📊 Stock Analysis for {symbol}")

    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    # 1️⃣ Price Trend with 7-Day MA
    with col1:
        st.markdown("**Price Trend with 7-Day Moving Avg**")
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.plot(filtered["timestamp"], filtered["price"], label="Price", color="blue")
        if "moving_avg_7d" in filtered.columns:
            ax.plot(filtered["timestamp"], filtered["moving_avg_7d"], label="7D MA", color="orange")
        ax.legend()
        st.pyplot(fig)

    # 2️⃣ Daily Return %
    with col2:
        if "daily_return_percent" in filtered.columns:
            st.markdown("**Daily Return %**")
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.bar(filtered["timestamp"], filtered["daily_return_percent"], color="green")
            ax.axhline(0, color="red", linestyle="--")
            st.pyplot(fig)

    # 3️⃣ Rolling Volatility
    with col3:
        if "rolling_std_7d" in filtered.columns:
            st.markdown("**Rolling Volatility (7D Std Dev)**")
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.plot(filtered["timestamp"], filtered["rolling_std_7d"], color="purple")
            st.pyplot(fig)

    # 4️⃣ Cumulative Return
    with col4:
        if "cumulative_return" in filtered.columns:
            st.markdown("**Cumulative Return**")
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.plot(filtered["timestamp"], filtered["cumulative_return"], color="black")
            st.pyplot(fig)

# ------------------- LIVE FEED -------------------
def show_live_feed():
    df = get_stock_data_from_mongodb(collection_name="prices", limit=100)
    if df.empty:
        st.warning("No live data found in MongoDB.")
        return
    st.subheader("🔴 Live Stock Prices")
    st.dataframe(df)

    if "symbol" in df.columns and "timestamp" in df.columns and "price" in df.columns:
        symbol = st.selectbox("Select a stock symbol", df["symbol"].unique())
        filtered = df[df["symbol"] == symbol].sort_values("timestamp")
        if not filtered.empty:
            st.line_chart(filtered.set_index("timestamp")["price"])

# ------------------- INSIGHTS & FORECAST -------------------
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
    except Exception:
        return pd.DataFrame()

def analyze_sentiment(text):
    text = ' '.join(text)
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0: return "Positive"
    elif analysis.sentiment.polarity < 0: return "Negative"
    else: return "Neutral"

def get_stock_sentiment(df):
    df['Sentiment'] = df['Headlines'].apply(analyze_sentiment)
    pos, neg = (df['Sentiment'] == 'Positive').sum(), (df['Sentiment'] == 'Negative').sum()
    if pos > neg: return 'green', "📈 Positive sentiment!"
    elif neg > pos: return 'red', "📉 Negative sentiment!"
    else: return 'orange', "➖ Neutral sentiment."

def get_stock_data_api(symbol):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=100&apikey=8a97b0338c804639931f90bff73311cc"
        response = requests.get(url)
        data = response.json()
        if 'values' not in data: return pd.DataFrame()
        df = pd.DataFrame(data['values'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df['close'] = df['close'].astype(float)
        df.sort_index(inplace=True)
        return df[['close']]
    except Exception:
        return pd.DataFrame()

def ets_demand_forecast(stock_data, forecast_period):
    model = ExponentialSmoothing(stock_data['close'], trend='add', seasonal='add', seasonal_periods=30)
    fit = model.fit()
    forecast = fit.forecast(steps=forecast_period)
    return forecast

def plot_demand_forecast(stock_data, forecast):
    fig, ax = plt.subplots()
    ax.plot(stock_data['close'], label='Historical', color='blue')
    ax.plot(pd.date_range(start=stock_data.index[-1], periods=len(forecast)+1, freq='B')[1:], forecast, label='Forecast', color='red')
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
        stock_data = get_stock_data_api(stock_symbol)
        if not stock_data.empty:
            st.line_chart(stock_data['close'])
            st.subheader("🔮 Forecast Stock Price")
            forecast_days = st.slider("Forecast Period (days)", 10, 180, 60)
            forecast = ets_demand_forecast(stock_data, forecast_days)
            plot_demand_forecast(stock_data, forecast)

# ------------------- CHATBOT -------------------
def show_chatbot():
    st.subheader("💬 AI Chatbot")
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "system", "content": "You are a helpful assistant."}]

    for msg in st.session_state.messages[1:]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask a question...")
    if user_input:
        st.chat_message("user").markdown(user_input)
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.chat_message("assistant"):
            response = ollama.chat(model="phi", messages=st.session_state.messages)
            reply = response['message']['content']
            st.markdown(reply)
        st.session_state.messages.append({"role": "assistant", "content": reply})

# ------------------- MAIN APP -------------------
def main():
    st.set_page_config(layout="wide", page_title="📊 Unified Stock Dashboard")
    st.title("📈 Stock Market Dashboard + 🤖 AI Assistant")

    # Theme toggle
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False
    if st.button("🌙 Toggle Dark/Light Mode"):
        st.session_state.dark_mode = not st.session_state.dark_mode
    set_theme(st.session_state.dark_mode)

    tab1, tab2, tab3, tab4 = st.tabs([
        "📡 Live Market Feed", 
        "📊 Insights & Forecast", 
        "📈 KPIs + Charts (MongoDB Features)", 
        "🤖 AI Chatbot"
    ])

    with tab1:
        show_live_feed()
    with tab2:
        show_stock_insights()
    with tab3:
        df = get_stock_data_from_mongodb(collection_name="stock_features", limit=200)
        if df.empty:
            st.warning("No stock features data found in MongoDB.")
        else:
            symbols = df["symbol"].unique()
            symbol = st.selectbox("Select Stock Symbol", symbols)
            show_kpis(df, symbol)
            show_charts(df, symbol)
    with tab4:
        show_chatbot()

if __name__ == "__main__":
    main()
