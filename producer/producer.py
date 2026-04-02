import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer

API_KEY = os.getenv("FINNHUB_API_KEY", "")
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "stock-quotes")

# Live mode controls
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "50"))
RUN_DURATION_SECONDS = int(os.getenv("RUN_DURATION_SECONDS", "300"))
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "6"))

# Backfill mode controls
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "60"))
BACKFILL_INTERVAL_MINUTES = int(os.getenv("BACKFILL_INTERVAL_MINUTES", "60"))
BACKFILL_SPEED_SECONDS = float(os.getenv("BACKFILL_SPEED_SECONDS", "0"))

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_live_quote(symbol):
    if not API_KEY:
        return None

    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        data["source"] = "live_api"
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


def should_stop(sent_count, start_time):
    if MAX_MESSAGES > 0 and sent_count >= MAX_MESSAGES:
        print(f"Reached MAX_MESSAGES limit ({MAX_MESSAGES})")
        return True
    if RUN_DURATION_SECONDS > 0 and (time.time() - start_time) >= RUN_DURATION_SECONDS:
        print(f"Reached RUN_DURATION_SECONDS limit ({RUN_DURATION_SECONDS}s)")
        return True
    return False


def build_synthetic_quote(symbol, ts_unix, last_price):
    drift = random.uniform(-0.02, 0.02)
    close_price = max(1.0, round(last_price * (1 + drift), 2))
    high_price = round(max(close_price, last_price) * (1 + random.uniform(0.0, 0.01)), 2)
    low_price = round(min(close_price, last_price) * (1 - random.uniform(0.0, 0.01)), 2)
    open_price = round(last_price, 2)

    return {
        "symbol": symbol,
        "c": close_price,
        "h": high_price,
        "l": low_price,
        "o": open_price,
        "pc": round(last_price, 2),
        "t": ts_unix,
        "fetched_at": ts_unix,
        "source": "synthetic_backfill"
    }, close_price


def run_live_mode():
    sent_count = 0
    start_time = time.time()
    print(
        f"Live producer started. limits: MAX_MESSAGES={MAX_MESSAGES}, "
        f"RUN_DURATION_SECONDS={RUN_DURATION_SECONDS}, interval={FETCH_INTERVAL_SECONDS}s"
    )

    while True:
        for symbol in SYMBOLS:
            if should_stop(sent_count, start_time):
                producer.flush()
                producer.close()
                print(f"Live producer stopped. Total messages sent: {sent_count}")
                return

            quote = fetch_live_quote(symbol)
            if quote:
                producer.send(TOPIC_NAME, value=quote)
                sent_count += 1
                print(f"[{sent_count}] Live sent: {symbol} @ {quote.get('c', 'N/A')}")

        time.sleep(FETCH_INTERVAL_SECONDS)


def run_backfill_mode(days, interval_minutes):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    step = timedelta(minutes=interval_minutes)

    if interval_minutes <= 0:
        raise ValueError("BACKFILL_INTERVAL_MINUTES must be > 0")

    base_prices = {
        "AAPL": 175.0,
        "MSFT": 410.0,
        "TSLA": 190.0,
        "GOOGL": 155.0,
        "AMZN": 180.0,
    }

    last_prices = {symbol: base_prices.get(symbol, 100.0) for symbol in SYMBOLS}
    sent_count = 0
    current = start

    print(
        f"Backfill producer started. days={days}, interval_minutes={interval_minutes}, "
        f"symbols={len(SYMBOLS)}"
    )

    while current <= now:
        ts_unix = int(current.timestamp())
        for symbol in SYMBOLS:
            payload, last_price = build_synthetic_quote(symbol, ts_unix, last_prices[symbol])
            last_prices[symbol] = last_price
            producer.send(TOPIC_NAME, value=payload)
            sent_count += 1

        if BACKFILL_SPEED_SECONDS > 0:
            time.sleep(BACKFILL_SPEED_SECONDS)

        current += step

    producer.flush()
    producer.close()
    print(f"Backfill completed. Total messages sent: {sent_count}")


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka stock quote producer")
    parser.add_argument(
        "--mode",
        choices=["live", "backfill"],
        default=os.getenv("PRODUCER_MODE", "live"),
        help="Producer mode: live (API) or backfill (synthetic historical)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=BACKFILL_DAYS,
        help="Number of days to generate for backfill mode",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=BACKFILL_INTERVAL_MINUTES,
        help="Backfill time step in minutes",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.mode == "backfill":
        run_backfill_mode(days=args.days, interval_minutes=args.interval_minutes)
        return

    run_live_mode()


if __name__ == "__main__":
    main()

