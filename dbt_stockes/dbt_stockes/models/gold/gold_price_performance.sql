{{ config(materialized='table') }}

-- Compare performance across symbols and time periods
WITH ranked_prices AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date ASC) AS row_num,
        FIRST_VALUE(close_price) OVER (PARTITION BY symbol ORDER BY trade_date ASC) AS first_price,
        LAST_VALUE(close_price) OVER (PARTITION BY symbol ORDER BY trade_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price
    FROM {{ ref('gold_daily_stock_summary') }}
)
SELECT
    symbol,
    trade_date,
    close_price,
    ROUND((close_price - first_price) / first_price * 100, 2) AS cumulative_return_pct,
    CASE
        WHEN (close_price - first_price) / first_price * 100 > 5 THEN 'Strong Gain'
        WHEN (close_price - first_price) / first_price * 100 > 0 THEN 'Moderate Gain'
        WHEN (close_price - first_price) / first_price * 100 > -5 THEN 'Moderate Loss'
        ELSE 'Strong Loss'
    END AS performance_rating,
    CURRENT_TIMESTAMP AS _loaded_at
FROM ranked_prices
ORDER BY symbol, trade_date
