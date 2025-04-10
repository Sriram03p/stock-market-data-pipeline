-- 1. Create Warehouse
CREATE OR REPLACE WAREHOUSE STOCK_WAREHOUSE
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- 2. Create Database
CREATE OR REPLACE DATABASE STOCK_DB;

-- 3. Create Schema
CREATE OR REPLACE SCHEMA STOCK_DB.STOCK_SCHEMA;

-- 4. Use context
USE DATABASE STOCK_DB;
USE SCHEMA STOCK_SCHEMA;
USE WAREHOUSE STOCK_WAREHOUSE;

-- 5. Create raw table for incoming data from Kafka
CREATE OR REPLACE TABLE stock_prices (
  symbol STRING,
  timestamp STRING,
  price FLOAT
);

CREATE OR REPLACE TABLE stock_prices_clean AS
SELECT DISTINCT
  symbol,
  TO_TIMESTAMP_NTZ(timestamp) AS timestamp,
  price
FROM stock_prices
WHERE TRY_CAST(price AS FLOAT) IS NOT NULL;

-- Create a task that runs daily at midnight
CREATE OR REPLACE TASK daily_stock_etl_task
  WAREHOUSE = STOCK_WAREHOUSE
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Every day at 12:00 AM UTC
AS
BEGIN
  MERGE INTO stock_prices_clean tgt
  USING (
    SELECT DISTINCT
      symbol,
      TO_TIMESTAMP_NTZ(timestamp) AS timestamp,
      price
    FROM stock_prices
    WHERE TRY_CAST(price AS FLOAT) IS NOT NULL
  ) src
  ON tgt.symbol = src.symbol AND tgt.timestamp = src.timestamp
  WHEN NOT MATCHED THEN
    INSERT (symbol, timestamp, price)
    VALUES (src.symbol, src.timestamp, src.price);
END;


