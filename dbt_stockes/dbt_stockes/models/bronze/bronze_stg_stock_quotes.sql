-- Extract OHLC data from JSONB payload and prepare for Silver transformation
SELECT
    id,
    symbol,
    fetched_at,
    (payload->>'c')::NUMERIC           AS current_price,
    (payload->>'d')::NUMERIC           AS change_amount,
    (payload->>'dp')::NUMERIC          AS change_percent,
    (payload->>'h')::NUMERIC           AS day_high,
    (payload->>'l')::NUMERIC           AS day_low,
    (payload->>'o')::NUMERIC           AS day_open,
    (payload->>'pc')::NUMERIC          AS prev_close,
    CASE
        WHEN LENGTH(payload->>'t') >= 13 THEN TO_TIMESTAMP((payload->>'t')::DOUBLE PRECISION / 1000)
        ELSE TO_TIMESTAMP((payload->>'t')::DOUBLE PRECISION)
    END AS market_timestamp,
    DATE(
        CASE
            WHEN LENGTH(payload->>'t') >= 13 THEN TO_TIMESTAMP((payload->>'t')::DOUBLE PRECISION / 1000)
            ELSE TO_TIMESTAMP((payload->>'t')::DOUBLE PRECISION)
        END
    ) AS trade_date,
    payload->>'source'                 AS data_source,
    source_file,
    inserted_at
FROM {{ source('raw', 'bronze_stock_quotes_raw') }}
WHERE payload IS NOT NULL
ORDER BY symbol, fetched_at
