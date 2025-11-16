from kafka import KafkaConsumer
import json
import time
from db import insert_error_row

BOOTSTRAP = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
TOPIC = "error-data"

def connect_consumer():
    for i in range(20):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        except:
            print(f"[WAIT] Kafka not ready... retry {i+1}/20")
            time.sleep(3)
    raise Exception("Kafka not available")

def main():
    consumer = connect_consumer()
    print("🔥 Error Consumer started. Listening to error-data...")

    for msg in consumer:
        try:
            error_data = msg.value
            print(f"[RECV] Error Message: {error_data}")

            insert_error_row(error_data)

            print("💾 Saved to Postgres")

        except Exception as e:
            print(f"❌ Failed to process message: {e}")
            continue

if __name__ == "__main__":
    main()
