import streamlit as st
import pandas as pd
import snowflake.connector
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from textblob import TextBlob
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sqlalchemy import create_engine

# ------------------- CONFIG -------------------
@st.cache_resource
def get_snowflake_engine():
    user = "SRIRAM"
    password = "TNMp7YBaBd9u6qJ"
    account = "UXUUHAA-GE37218"
    warehouse = "STOCK_WAREHOUSE"
    database = "STOCK_DB"
    schema = "STOCK_SCHEMA"
    url = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    engine = create_engine(url)
    return engine

# ------------------- TAB 1: LIVE STOCK FEED -------------------
def show_live_feed():
    engine = get_snowflake_engine()
    query = """
        SELECT * FROM stock_prices_clean
        ORDER BY timestamp DESC
        LIMIT 100
    """
    df = pd.read_sql(query, engine)

    st.subheader("🔴 Live Stock Prices")
    st.dataframe(df)

    symbol = st.selectbox("Select a stock symbol", df["symbol"].unique())
    filtered = df[df["symbol"] == symbol].sort_values("timestamp")
    st.line_chart(filtered.set_index("timestamp")["price"])

# ------------------- TAB 2: INSIGHTS + FORECAST -------------------
ALPHA_VANTAGE_API_KEY = 'B5WJBWUZC8XHRVOF'
TWELVE_DATA_API_KEY = '8a97b0338c804639931f90bff73311cc'

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
        # News
        st.subheader("📰 News Headlines")
        news_df = get_stock_news(stock_symbol)
        if not news_df.empty:
            st.dataframe(news_df)
            color, message = get_stock_sentiment(news_df)
            st.markdown(f'<p style="color:{color}"><b>{message}</b></p>', unsafe_allow_html=True)

        # Historical data
        st.subheader("📊 Historical Stock Data")
        stock_data = get_stock_data(stock_symbol)
        if not stock_data.empty:
            st.line_chart(stock_data['Close'])

            # Forecast
            st.subheader("🔮 Forecast Stock Price")
            forecast_days = st.slider("Forecast Period (days)", 10, 180, 60)
            forecast = ets_demand_forecast(stock_data, forecast_days)
            plot_demand_forecast(stock_data, forecast)

# ------------------- MAIN APP -------------------
def main():
    st.set_page_config(layout="wide", page_title="📈 Stock Market Dashboard")
    st.title("📊 Real-Time Stock Dashboard")

    tab1, tab2 = st.tabs(["📡 Live Market Feed", "🧠 Stock Insights & Forecast"])
    with tab1:
        show_live_feed()
    with tab2:
        show_stock_insights()

if __name__ == "__main__":
    main()
