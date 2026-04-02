{{ config(materialized='table') }}

WITH hourly_data AS (
    SELECT
        symbol,
        DATE_TRUNC('hour', market_timestamp) AS hour_timestamp,
        COUNT(*)                                   AS quote_count,
        ROUND(MIN(current_price), 2)              AS hour_low,
        ROUND(MAX(current_price), 2)              AS hour_high,
        ROUND(AVG(current_price), 2)              AS hour_avg,
        ROUND(STDDEV_POP(current_price), 2)       AS price_volatility,
        ROUND(AVG(change_percent), 4)             AS avg_change_percent
    FROM {{ ref('silver_clean_stock_quotes') }}
    WHERE current_price > 0
    GROUP BY symbol, DATE_TRUNC('hour', market_timestamp)
)
SELECT
    hour_timestamp,
    DATE(hour_timestamp) AS trade_date,
    EXTRACT(HOUR FROM hour_timestamp) AS hour_of_day,
    symbol,
    quote_count,
    hour_low,
    hour_high,
    hour_avg,
    ROUND(hour_high - hour_low, 2) AS intraday_range,
    price_volatility,
    avg_change_percent,
    CURRENT_TIMESTAMP AS _loaded_at
FROM hourly_data
ORDER BY hour_timestamp DESC, symbol
