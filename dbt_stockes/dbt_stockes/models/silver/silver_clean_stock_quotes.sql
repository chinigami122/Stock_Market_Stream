-- Clean and normalize stock quotes; drop duplicates and invalid records
{{ config(materialized='table') }}

WITH deduplicated AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY symbol, market_timestamp ORDER BY fetched_at DESC) AS rn,
        id,
        symbol,
        current_price,
        ROUND(day_high, 2)        AS day_high,
        ROUND(day_low, 2)         AS day_low,
        ROUND(day_open, 2)        AS day_open,
        ROUND(prev_close, 2)      AS prev_close,
        change_amount,
        ROUND(change_percent, 4)  AS change_percent,
        market_timestamp,
        fetched_at,
        data_source,
        source_file,
        CURRENT_TIMESTAMP       AS _loaded_at
    FROM {{ ref('bronze_stg_stock_quotes') }}
    WHERE current_price > 0
      AND market_timestamp IS NOT NULL
      AND symbol IS NOT NULL
)
SELECT
    id,
    symbol,
    current_price,
    day_high,
    day_low,
    day_open,
    prev_close,
    change_amount,
    change_percent,
    market_timestamp,
    fetched_at,
    data_source,
    source_file,
    _loaded_at
FROM deduplicated
WHERE rn = 1
ORDER BY symbol, market_timestamp
