# Stock Market Analytics dBT Project

## Overview
This dBT project transforms raw stock market quote data from PostgreSQL into analytics-ready tables optimized for Power BI dashboards.

## Architecture

### Bronze Layer (Raw/Staging)
- **bronze_stg_stock_quotes**: Extracts OHLC data from raw JSONB payloads
- Source: `analytics.bronze_stock_quotes_raw` (PostgreSQL)
- Contains: symbol, OHLC prices, timestamps, and metadata

### Silver Layer (Cleaned)
- **silver_clean_stock_quotes**: Deduplicates, validates, and normalizes data
- Drops invalid records (null prices, invalid timestamps)
- Removes duplicates keeping latest version per market_timestamp
- Materialized as table for performance

### Gold Layer (Aggregated/Dashboard-Ready)
1. **gold_daily_stock_summary**: Daily OHLC summary by symbol
   - Use for: Daily trend analysis, historical price data
   - Primary for Power BI daily charts

2. **gold_hourly_stock_metrics**: Hourly price aggregations
   - Use for: Intraday trends, volatility analysis
   - Columns: hour_timestamp, quote_count, hour_low/high/avg, price_volatility

3. **gold_kpi.sql**: Latest KPIs and rolling window metrics
   - Use for: KPI cards, scorecards in Power BI
   - Includes 7-day, 30-day, and YTD volatility and averages

4. **gold_candlestick_data.sql**: Candlestick chart data
   - Use for: Candlestick/OHLC charts in Power BI
   - Includes body, wicks, and direction (UP/DOWN/FLAT)

5. **gold_price_performance.sql**: Performance ranking and cumulative returns
   - Use for: Comparative performance, ranking, gauge charts

## Configuration

### PostgreSQL Connection
Update `profiles.yml` in your dBT profiles folder (~/.dbt/profiles.yml):

```yaml
dbt_stockes:
  target: postgres
  outputs:
    postgres:
      type: postgres
      host: localhost
      port: 5432
      user: airflow
      password: airflow
      dbname: airflow
      schema: analytics
      threads: 4
```

## Running dBT

### Build all models
```bash
dbt run
```

### Run tests
```bash
dbt test
```

### Generate documentation
```bash
dbt docs generate
dbt docs serve
```

### Build and test in one command
```bash
dbt build
```

## Data Quality Notes
- Validates that stock prices > 0
- Removes duplicate records from same timestamp
- Ensures symbol and timestamp are non-null
- Extracted from JSONB `payload` column in raw table

## Power BI Integration
1. Connect to PostgreSQL analytics schema
2. Use gold_* tables for visualizations
3. Refresh schedule: After each DAG run that loads new data
4. Key date field: `trade_date` (available in all gold tables)
5. Key dimension: `symbol` (available in all gold tables)

## Troubleshooting

### Models failing with "Table not found"
- Ensure Airflow DAG has run and populated bronze_stock_quotes_raw
- Verify PostgreSQL connection in profiles.yml

### JSONB extraction errors
- Check that payload column contains valid JSON
- Run DAG's ensure_bucket_exists and download_from_minio tasks first

### Duplicate rows in silver layer
- This is expected; deduplication keeps only the latest quote per timestamp
