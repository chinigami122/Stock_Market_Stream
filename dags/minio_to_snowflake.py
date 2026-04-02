"""
DAG: minio_to_snowflake_bronze
Purpose: Load raw JSON files from MinIO (Bronze) into Snowflake (Bronze).
Trigger: Manual (after ingestion window).
"""

import os
import json
import boto3
import snowflake.connector
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timezone

# === MinIO Config ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET = os.getenv("BUCKET", "bronze-transactions")
LOCAL_DIR = os.getenv("LOCAL_DIR", "/tmp/minio_bronze")

# === Snowflake Config ===
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "STOCKS_MDS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "COMMON")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "BRONZE_STOCK_QUOTES_RAW")

# === Warehouse Target Config ===
TARGET_WAREHOUSE = os.getenv("TARGET_WAREHOUSE", "postgres").lower()

# === PostgreSQL Target Config ===
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "analytics")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "bronze_stock_quotes_raw")


def ensure_bucket_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' is ready.")
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchBucket", "NotFound"}:
            s3.create_bucket(Bucket=BUCKET)
            print(f"Bucket '{BUCKET}' was missing and has been created.")
            return
        raise AirflowFailException(f"Failed checking bucket '{BUCKET}': {e}")


def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    paginator = s3.get_paginator("list_objects_v2")
    local_files = []

    for page in paginator.paginate(Bucket=BUCKET):
        objects = page.get("Contents", [])
        if not objects:
            continue

        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, key.replace("/", os.sep))
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            s3.download_file(BUCKET, key, local_file)
            local_files.append(local_file)

    if not local_files:
        print("No files found in MinIO.")
        return []

    print(f"Downloaded {len(local_files)} files from MinIO.")
    return local_files


def load_to_snowflake(local_files):
    if not local_files:
        print("Nothing to load into Snowflake.")
        return

    if not (SNOWFLAKE_USER and SNOWFLAKE_PASSWORD and SNOWFLAKE_ACCOUNT):
        raise AirflowFailException(
            "Snowflake environment variables are missing. Set SNOWFLAKE_USER, "
            "SNOWFLAKE_PASSWORD, and SNOWFLAKE_ACCOUNT."
        )

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA
        )
    except Exception as e:
        raise AirflowFailException(f"Snowflake connection failed: {e}")

    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%{SNOWFLAKE_TABLE}")

    cur.execute(f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @%{SNOWFLAKE_TABLE}
        FILE_FORMAT = (TYPE = JSON)
    """)

    cur.close()
    conn.close()
    print("Bronze load completed in Snowflake.")


def _to_timestamp(value):
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def load_to_postgres(local_files):
    if not local_files:
        print("Nothing to load into PostgreSQL.")
        return

    try:
        import psycopg2
        from psycopg2.extras import Json
    except Exception as e:
        raise AirflowFailException(f"PostgreSQL driver import failed: {e}")

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
    except Exception as e:
        raise AirflowFailException(f"PostgreSQL connection failed: {e}")

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT,
            fetched_at TIMESTAMPTZ,
            payload JSONB NOT NULL,
            source_file TEXT,
            inserted_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )

    inserted_rows = 0
    for f in local_files:
        try:
            with open(f, "r", encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
        except Exception as e:
            print(f"Skipping invalid JSON file '{f}': {e}")
            continue

        symbol = payload.get("symbol")
        fetched_at = _to_timestamp(payload.get("fetched_at"))
        cur.execute(
            f"""
            INSERT INTO {POSTGRES_SCHEMA}.{POSTGRES_TABLE}
            (symbol, fetched_at, payload, source_file)
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, fetched_at, Json(payload), f)
        )
        inserted_rows += 1

    cur.close()
    conn.close()
    print(f"Bronze load completed in PostgreSQL. Inserted rows: {inserted_rows}")


def load_to_warehouse(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_from_minio")
    print(f"Selected warehouse target: {TARGET_WAREHOUSE}")

    if TARGET_WAREHOUSE == "snowflake":
        load_to_snowflake(local_files)
        return

    if TARGET_WAREHOUSE == "postgres":
        load_to_postgres(local_files)
        return

    raise AirflowFailException(
        "Invalid TARGET_WAREHOUSE. Use 'postgres' or 'snowflake'."
    )


with DAG(
    dag_id="minio_to_snowflake_bronze",
    start_date=datetime(2026, 2, 5),
    schedule_interval=None,   # manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["bronze"]
) as dag:

    ensure_bucket_task = PythonOperator(
        task_id="ensure_bucket_exists",
        python_callable=ensure_bucket_exists
    )

    download_task = PythonOperator(
        task_id="download_from_minio",
        python_callable=download_from_minio
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse
    )

    ensure_bucket_task >> download_task >> load_task
