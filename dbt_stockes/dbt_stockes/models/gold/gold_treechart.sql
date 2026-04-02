{{ config(materialized='table') }}

-- Stock performance summary for hierarchical visualizations
WITH symbol_stats AS (
    SELECT
        symbol,
        COUNT(DISTINCT DATE(market_timestamp)) AS trading_days,
        ROUND(AVG(current_price)::NUMERIC, 2) AS avg_price_all_time,
        ROUND(MIN(current_price)::NUMERIC, 2) AS min_price,
        ROUND(MAX(current_price)::NUMERIC, 2) AS max_price,
        ROUND(STDDEV_POP(current_price)::NUMERIC, 4) AS volatility
    FROM {{ ref('silver_clean_stock_quotes') }}
    GROUP BY symbol
)

SELECT
    symbol,
    avg_price_all_time,
    min_price,
    max_price,
    volatility,
    trading_days,
    ROUND((volatility / NULLIF(avg_price_all_time, 0)), 4) AS relative_volatility,
    CURRENT_TIMESTAMP AS _created_at
FROM symbol_stats
ORDER BY avg_price_all_time DESC, symbol