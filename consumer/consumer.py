from kafka import KafkaConsumer
import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)

consumer = KafkaConsumer(
    'crypto-stream',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-group'
)

conn = psycopg2.connect(
    dbname="crypto",
    user="postgres",
    password="postgres",
    host="db",
    port="5432"
)
cur = conn.cursor()

for msg in consumer:
    data = msg.value
    try:
        cur.execute(
            "INSERT INTO prices (id, symbol, price, volume, timestamp) VALUES (%s, %s, %s, %s, %s)",
            (data["id"], data["symbol"], data["price"], data.get("volume", 0), data["timestamp"])
        )
        conn.commit()
        logging.info(f"📥 Received: {data}")
    except Exception as e:
        logging.error(f"❌ DB error: {e}")
