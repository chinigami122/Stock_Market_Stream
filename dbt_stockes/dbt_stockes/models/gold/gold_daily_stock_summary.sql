{{ config(materialized='table') }}

WITH daily_data AS (
    SELECT
        symbol,
        DATE(market_timestamp) AS trade_date,
        COUNT(*) AS quote_count,
        ROUND(MIN(current_price)::NUMERIC, 2) AS low_price,
        ROUND(MAX(current_price)::NUMERIC, 2) AS high_price,
        ROUND(AVG(current_price)::NUMERIC, 2) AS avg_price,
        ROUND(STDDEV_POP(current_price)::NUMERIC, 2) AS price_volatility,
        ROUND(MAX(ABS(change_percent))::NUMERIC, 4) AS max_volatility,
        ROUND(AVG(change_percent)::NUMERIC, 4) AS avg_change_percent
    FROM {{ ref('silver_clean_stock_quotes') }}
    GROUP BY symbol, DATE(market_timestamp)
),

with_ohlc AS (
    SELECT
        dd.trade_date,
        dd.symbol,
        ROUND(MIN(sq.current_price)::NUMERIC, 2) AS open_price,
        dd.low_price,
        dd.high_price,
        ROUND(MAX(sq.current_price)::NUMERIC, 2) AS close_price,
        dd.quote_count,
        dd.avg_price,
        ROUND(dd.high_price - ROUND(MIN(sq.current_price)::NUMERIC, 2), 2) AS price_range,
        dd.avg_change_percent,
        dd.max_volatility
    FROM daily_data dd
    LEFT JOIN {{ ref('silver_clean_stock_quotes') }} sq 
        ON dd.symbol = sq.symbol AND DATE(sq.market_timestamp) = dd.trade_date
    GROUP BY dd.trade_date, dd.symbol, dd.low_price, dd.high_price, dd.quote_count, dd.avg_price, dd.avg_change_percent, dd.max_volatility
)

SELECT
    trade_date,
    symbol,
    open_price,
    high_price,
    low_price,
    close_price,
    ROUND((COALESCE(close_price, open_price) - COALESCE(open_price, close_price)) / NULLIF(COALESCE(open_price, close_price), 0) * 100, 2) AS daily_change_percent,
    price_range,
    quote_count,
    avg_price,
    avg_change_percent,
    max_volatility,
    CURRENT_TIMESTAMP AS _loaded_at
FROM with_ohlc
ORDER BY trade_date DESC, symbol
