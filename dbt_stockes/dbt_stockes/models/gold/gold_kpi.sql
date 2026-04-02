{{ config(materialized='table') }}

WITH latest_day AS (
    SELECT
        symbol,
        MAX(trade_date) AS latest_date
    FROM {{ ref('gold_daily_stock_summary') }}
    GROUP BY symbol
)
SELECT
    CURRENT_DATE AS compute_date,
    ds.symbol,
    
    -- Latest trading day metrics
    ds.close_price AS latest_close_price,
    ds.daily_change_percent AS latest_daily_change_pct,
    ds.high_price AS latest_high,
    ds.low_price AS latest_low,
    
    -- 7-day metrics
    ROUND(AVG(ds2.close_price), 2) AS avg_close_7d,
    MAX(ds2.high_price) AS high_7d,
    MIN(ds2.low_price) AS low_7d,
    ROUND(STDDEV_POP(ds2.close_price), 2) AS volatility_7d,
    
    -- 30-day metrics
    ROUND(AVG(ds3.close_price), 2) AS avg_close_30d,
    MAX(ds3.high_price) AS high_30d,
    MIN(ds3.low_price) AS low_30d,
    ROUND(STDDEV_POP(ds3.close_price), 2) AS volatility_30d,
    
    CURRENT_TIMESTAMP AS _loaded_at
FROM latest_day ld
JOIN {{ ref('gold_daily_stock_summary') }} ds ON ld.symbol = ds.symbol AND ld.latest_date = ds.trade_date
LEFT JOIN {{ ref('gold_daily_stock_summary') }} ds2 ON ds.symbol = ds2.symbol AND ds2.trade_date >= CURRENT_DATE - INTERVAL '7 days'
LEFT JOIN {{ ref('gold_daily_stock_summary') }} ds3 ON ds.symbol = ds3.symbol AND ds3.trade_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY ld.symbol, ld.latest_date, ds.symbol, ds.close_price, ds.daily_change_percent, ds.high_price, ds.low_price