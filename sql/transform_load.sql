USE DATABASE STOCK_DB;
USE SCHEMA STOCK_SCHEMA;
USE WAREHOUSE STOCK_WAREHOUSE;

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

ALTER TASK daily_stock_etl_task RESUME;