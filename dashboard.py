import streamlit as st
import pandas as pd
import snowflake.connector
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from textblob import TextBlob
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from statsmodels.tsa.arima.model import ARIMA
from pmdarima.model_selection import train_test_split
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# --- SNOWFLAKE CONNECTION ---
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='YOUR_USER',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT',
        warehouse='STOCK_WAREHOUSE',
        database='STOCK_DB',
        schema='STOCK_SCHEMA'
    )

# --- NEWS HEADLINES ---
def get_stock_news(stock_symbol):
    url = f'https://finance.yahoo.com/quote/{stock_symbol}?p={stock_symbol}'
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        headlines = [headline.get_text() for headline in soup.find_all('h3')]
        return pd.DataFrame({'Headlines': headlines[:10]})
    else:
        return pd.DataFrame()

def analyze_sentiment(text):
    text = ' '.join(text)
    sentiment = TextBlob(text).sentiment.polarity
    if sentiment > 0:
        return "Positive"
    elif sentiment < 0:
        return "Negative"
    else:
        return "Neutral"

def get_stock_sentiment(df):
    df['Sentiment'] = df['Headlines'].apply(analyze_sentiment)
    counts = df['Sentiment'].value_counts()
    if counts.get("Positive", 0) > counts.get("Negative", 0):
        return 'green', "📈 Positive sentiment detected"
    elif counts.get("Negative", 0) > counts.get("Positive", 0):
        return 'red', "📉 Negative sentiment detected"
    else:
        return 'orange', "➖ Neutral sentiment"

# --- HISTORICAL DATA ---
def get_previous_year_data(stock_symbol):
    try:
        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        return yf.download(stock_symbol, start=start_date, end=end_date)
    except:
        return pd.DataFrame()

# --- FORECASTING ---
def ets_demand_forecast(data, days):
    model = ExponentialSmoothing(data['Close'], trend='add', seasonal='add', seasonal_periods=30)
    fit = model.fit()
    return fit.forecast(steps=days)

def plot_demand_forecast(data, forecast):
    fig, ax = plt.subplots()
    ax.plot(data['Close'], label='Close Price')
    ax.plot(pd.date_range(start=data.index[-1], periods=len(forecast)+1, freq='B')[1:], forecast, label='Forecast', color='red')
    ax.set_title("📊 Stock Price Forecast")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend()
    st.pyplot(fig)

# --- MAIN APP ---
def main():
    st.set_page_config(layout="wide", page_title="📈 Stock Market Dashboard")

    st.title("📊 Real-Time Stock Dashboard")

    tab1, tab2 = st.tabs(["📡 Live Market Feed", "🧠 Stock Insights & Forecast"])

    # --- TAB 1: REAL-TIME FEED ---
    with tab1:
        conn = get_snowflake_connection()
        query = """
            SELECT * FROM stock_prices_clean
            ORDER BY timestamp DESC
            LIMIT 100
        """
        df = pd.read_sql(query, conn)
        st.subheader("🔴 Live Stock Prices")
        st.dataframe(df)

        symbol = st.selectbox("Select a stock symbol", df["SYMBOL"].unique())
        filtered = df[df["SYMBOL"] == symbol].sort_values("TIMESTAMP")
        st.line_chart(filtered.set_index("TIMESTAMP")["PRICE"])

    # --- TAB 2: INSIGHTS ---
    with tab2:
        stock_symbol = st.text_input("Enter Stock Symbol (e.g., AAPL, TSLA)").upper()
        if st.button("Analyze") and stock_symbol:
            # --- News & Sentiment ---
            st.subheader(f"📰 Latest News: {stock_symbol}")
            news_df = get_stock_news(stock_symbol)
            if not news_df.empty:
                st.dataframe(news_df)
                color, message = get_stock_sentiment(news_df)
                st.markdown(f"<p style='color:{color}'><b>{message}</b></p>", unsafe_allow_html=True)
            else:
                st.warning("No news found.")

            # --- Historical & Forecast ---
            st.subheader(f"📉 Historical Trend & Forecast: {stock_symbol}")
            data = get_previous_year_data(stock_symbol)
            if not data.empty:
                st.line_chart(data['Close'])
                forecast_days = st.slider("Forecast Period (days)", 1, 180, 30)
                forecast = ets_demand_forecast(data, forecast_days)
                plot_demand_forecast(data, forecast)
            else:
                st.warning("No historical data found.")

if __name__ == "__main__":
    main()
