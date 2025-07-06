import requests
import json
import time
from kafka import KafkaProducer

# Kafka settings (match your docker-compose config)
KAFKA_BROKER = 'kafka:9092'  # Inside Docker
TOPIC = 'crypto-stream'

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum"
    }

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            for coin in data:
                payload = {
                    "id": coin["id"],
                    "symbol": coin["symbol"],
                    "price": coin["current_price"],
                    "volume": coin["total_volume"],
                    "timestamp": coin["last_updated"]
                }

                # Send to Kafka topic
                producer.send(TOPIC, value=payload)
                print(f"[+] Sent: {payload['id']} | Price: {payload['price']}")
            
            time.sleep(5)

        except Exception as e:
            print(f"[!] Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    print("[*] Starting Kafka Producer...")
    fetch_and_send()
