# 📈 Real-Time Stock Market Data Pipeline

A real-time data pipeline to ingest, process, and visualize live stock market data using **Kafka**, **Snowflake**, and **Streamlit**.

---

## 🚀 Overview

This project simulates a real-world data engineering use case:

- Fetches real-time stock data from Alpha Vantage API.
- Streams the data into **Kafka** (via Docker).
- Consumes and stores data into **Snowflake** (cloud data warehouse).
- Cleans and transforms the data via SQL Tasks.
- Visualizes live prices in a **Streamlit Dashboard**.
- Triggers alerts when stock prices exceed defined thresholds.

---

## 🧱 Architecture

```mermaid
graph LR
    A[Alpha Vantage API] --> B[Kafka Producer]
    B --> C[Kafka Topic: stock_prices]
    C --> D[Kafka Consumer]
    D --> E[Snowflake Table: stock_prices]
    E --> F[Snowflake Task: Clean & Deduplicate]
    E --> G[Streamlit Dashboard]
