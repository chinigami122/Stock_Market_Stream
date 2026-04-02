{{ config(materialized='table') }}

-- Dataset ready for candlestick charts in Power BI
SELECT
    trade_date,
    symbol,
    open_price,
    close_price,
    high_price,
    low_price,
    daily_change_percent,
    CASE
        WHEN close_price > open_price THEN 'UP'
        WHEN close_price < open_price THEN 'DOWN'
        ELSE 'FLAT'
    END AS direction,
    ROUND(ABS(close_price - open_price), 2) AS body,
    ROUND(high_price - GREATEST(open_price, close_price), 2) AS upper_wick,
    ROUND(LEAST(open_price, close_price) - low_price, 2) AS lower_wick,
    quote_count,
    avg_price,
    price_range,
    max_volatility
FROM {{ ref('gold_daily_stock_summary') }}
ORDER BY trade_date DESC, symbol
