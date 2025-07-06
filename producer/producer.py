from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "https://api.coingecko.com/api/v3/simple/price"
ASSETS = {"bitcoin": "BIT", "ethereum": "ETH", "cardano": "CAR"}

def fetch_data():
    try:
        response = requests.get(API_URL, params={
            'ids': ','.join(ASSETS.keys()),
            'vs_currencies': 'usd'
        })
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"❌ API fetch error: {e}")
        return {}

while True:
    data = fetch_data()
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    for coin, symbol in ASSETS.items():
        if coin in data and "usd" in data[coin]:
            message = {
                "id": coin,
                "symbol": symbol,
                "price": data[coin]["usd"],
                "volume": 0,
                "timestamp": now
            }
            producer.send("crypto-stream", value=message)
            logging.info(f"✅ Sent: {message}")
        else:
            logging.warning(f"⚠️ Missing data for {coin}")
    time.sleep(10)
