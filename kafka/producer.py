import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'crypto-stream'

ASSETS = {
    "bitcoin": 0,
    "ethereum": 1,
    "cardano": 2
}

API_URL = "https://api.coingecko.com/api/v3/simple/price"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_prices():
    try:
        params = {
            "ids": ",".join(ASSETS.keys()),
            "vs_currencies": "usd",
            "include_24hr_vol": "true",
            "include_last_updated_at": "true"
        }
        response = requests.get(API_URL, params=params)

        if response.status_code == 429:
            print("🚫 Rate limit hit. Sleeping for 60s...")
            time.sleep(60)
            return {}

        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"❌ API error: {e}")
        time.sleep(10)
        return {}

def fetch_and_send():
    while True:
        prices = fetch_prices()
        if not prices:
            continue

        for coin, data in prices.items():
            try:
                timestamp_unix = data.get("last_updated_at")
                timestamp_iso = datetime.utcfromtimestamp(timestamp_unix).isoformat() if timestamp_unix else None

                message = {
                    "id": coin,
                    "symbol": coin[:3],
                    "price": data.get("usd"),
                    "volume": data.get("usd_24h_vol"),
                    "timestamp": timestamp_iso
                }

                producer.send(TOPIC, value=message)
                producer.flush()  # Ensure the message is sent immediately
                print(f"✅ Sent to Kafka: {message['id'].upper()} | Price: ${message['price']}")

            except Exception as e:
                print(f"❌ Kafka error while sending {coin}: {e}")

        time.sleep(10)

if __name__ == "__main__":
    print("🚀 Kafka Producer started...")
    fetch_and_send()
