{{ config(materialized='table') }}

-- Candlestick data for Power BI charts
WITH quote_daily AS (
    SELECT
        symbol,
        DATE(market_timestamp) AS trade_date,
        MIN(current_price) AS daily_low,
        MAX(current_price) AS daily_high,
        ROUND(AVG(current_price)::NUMERIC, 2) AS daily_avg,
        COUNT(*) AS quote_count
    FROM {{ ref('silver_clean_stock_quotes') }}
    GROUP BY symbol, DATE(market_timestamp)
)

SELECT
    symbol,
    trade_date AS candle_time,
    daily_low AS candle_low,
    daily_high AS candle_high,
    daily_high - daily_low AS price_range,
    daily_avg AS candle_mid,
    quote_count,
    daily_avg AS avg_price,
    CURRENT_TIMESTAMP AS _loaded_at
FROM quote_daily
ORDER BY symbol, trade_date DESC