import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# ------------------- CONFIG -------------------
@st.cache_resource
def get_mongodb_client():
    client = MongoClient("mongodb://localhost:27017/")  # Update if remote
    return client

def get_stock_data_from_mongodb():
    client = get_mongodb_client()
    db = client["stock_data"]
    collection = db["stock_features"]

    cursor = collection.find().sort("TIMESTAMP", -1).limit(200)
    data = list(cursor)

    if data:
        df = pd.DataFrame(data)
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)

        df.columns = df.columns.str.lower()

        # Convert timestamp
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
            body {
                background-color: #1E1E1E;
                color: #E0E0E0;
            }
            .stApp {
                background-color: #1E1E1E;
                color: #E0E0E0;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <style>
            body {
                background-color: #FFFFFF;
                color: #000000;
            }
            .stApp {
                background-color: #FFFFFF;
                color: #000000;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

# ------------------- KPI CARDS -------------------
def show_kpis(df, symbol):
    latest = df[df["symbol"] == symbol].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("📈 Last Price", f"${latest['price']:.2f}")
    col2.metric("🔄 Daily Change", f"{latest['daily_change']:.2f}")
    col3.metric("📊 Daily Return %", f"{latest['daily_return_percent']:.2f}%")

    col4, col5 = st.columns(2)
    col4.metric("📉 7D Moving Avg", f"${latest['moving_avg_7d']:.2f}")
    col5.metric("💰 Cumulative Return", f"{latest['cumulative_return']:.2f}%")

# ------------------- CHARTS -------------------
def show_charts(df, symbol):
    filtered = df[df["symbol"] == symbol]

    st.subheader(f"📊 Stock Analysis for {symbol}")

    # Create 2x2 layout
    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    # 1️⃣ Price Trend with 7-Day Moving Average
    with col1:
        st.markdown("**Price Trend with 7-Day Moving Avg**")
        st.caption(
            "Daily stock price (blue) with a 7-day moving average (orange). "
            "The moving average smooths out short-term fluctuations and highlights the trend."
        )
        fig, ax = plt.subplots(figsize=(6, 3))  # smaller figure
        ax.plot(filtered["timestamp"], filtered["price"], label="Price", color="blue")
        if "moving_avg_7d" in filtered.columns:
            ax.plot(filtered["timestamp"], filtered["moving_avg_7d"], label="7D MA", color="orange")
        ax.set_xlabel("Date")
        ax.set_ylabel("Price (USD)")
        ax.legend()
        st.pyplot(fig)

    # 2️⃣ Daily Return %
    with col2:
        if "daily_return_percent" in filtered.columns:
            st.markdown("**Daily Return %**")
            st.caption(
                "Percentage return for each day. "
                "Green bars represent gains; red dashed line marks zero baseline."
            )
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.bar(filtered["timestamp"], filtered["daily_return_percent"], color="green")
            ax.axhline(0, color="red", linestyle="--")
            ax.set_ylabel("Return %")
            st.pyplot(fig)

    # 3️⃣ Rolling Volatility
    with col3:
        if "rolling_std_7d" in filtered.columns:
            st.markdown("**Rolling Volatility (7-Day Std Dev)**")
            st.caption(
                "7-day rolling standard deviation of returns. "
                "Higher values indicate more fluctuations (risk/volatility)."
            )
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.plot(filtered["timestamp"], filtered["rolling_std_7d"], color="purple")
            ax.set_ylabel("Std Dev")
            st.pyplot(fig)

    # 4️⃣ Cumulative Return
    with col4:
        if "cumulative_return" in filtered.columns:
            st.markdown("**Cumulative Return**")
            st.caption(
                "Total accumulated return since start date. "
                "Helps visualize overall stock performance."
            )
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.plot(filtered["timestamp"], filtered["cumulative_return"], color="black")
            ax.set_ylabel("Return %")
            st.pyplot(fig)


# ------------------- MAIN APP -------------------
def main():
    st.set_page_config(layout="wide", page_title="📊 Enhanced Stock Dashboard")

    # Theme toggle
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False

    if st.button("🌙 Toggle Dark/Light Mode"):
        st.session_state.dark_mode = not st.session_state.dark_mode
    set_theme(st.session_state.dark_mode)

    st.title("📈 Stock Market Dashboard with KPIs + Charts")

    df = get_stock_data_from_mongodb()
    if df.empty:
        st.warning("No data found in MongoDB.")
        return

    symbols = df["symbol"].unique()
    symbol = st.selectbox("Select Stock Symbol", symbols)

    # Show KPIs
    show_kpis(df, symbol)

    # Show charts
    show_charts(df, symbol)

if __name__ == "__main__":
    main()
