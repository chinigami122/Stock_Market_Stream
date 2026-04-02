import json 
import boto3
import time
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = os.getenv("BUCKET", "bronze-transactions")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-consumer1")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} already exists.")
except Exception:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"Created bucket {BUCKET_NAME}.")


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=90000  # Exit after 10 seconds of no messages
)

print("Consumerstreaming and saving to MinIO...")

for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at",int(time.time()))
    # Convert Unix timestamp to date string for partitioning
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    # Hive-style partitioned path: symbol=AAPL/dt=2026-02-05/1707141234.json
    key = f"symbol={symbol}/dt={dt}/{ts}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{BUCKET_NAME}/{key}")