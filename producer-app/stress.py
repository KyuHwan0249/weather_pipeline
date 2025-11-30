import argparse
import json
import time
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]

LOCATIONS = [
    "Seoul", "New York", "London", "Tokyo",
    "Paris", "Busan", "Jeju", "Dallas", "San Diego"
]

# ------------------------------------------------
# Producer
# ------------------------------------------------
def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=0,
        linger_ms=3,
        batch_size=131072
    )


# ------------------------------------------------
# weather / retry 스키마
# ------------------------------------------------
def generate_weather_like(retry=False):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    location = random.choice(LOCATIONS)

    return {
        "Location": location,
        "Date_Time": now,
        "Temperature_C": round(random.uniform(-20, 40), 2),
        "Humidity_pct": round(random.uniform(20, 90), 1),
        "Precipitation_mm": round(random.uniform(0, 50), 1),
        "Wind_Speed_kmh": round(random.uniform(0, 20), 1),
        "retry": random.randint(1, 5) if retry else 0
    }


# ------------------------------------------------
# error 스키마들
# ------------------------------------------------
def generate_error_missing_field():
    return {
        "error_type": "MISSING_FIELD",
        "error_reason": "Missing required field: Humidity_pct",
        "raw_row": {
            "Location": random.choice(LOCATIONS),
            "Date_Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Temperature_C": str(round(random.uniform(10, 35), 3)),
            "Humidity_pct": "",
            "Precipitation_mm": str(round(random.uniform(0, 15), 3)),
            "Wind_Speed_kmh": str(round(random.uniform(0, 15), 3)),
            "retry": 0
        },
        "timestamp": time.time(),
        "file_name": "stress_error_test.csv",
        "retry_count": 0
    }


def generate_error_type_error():
    return {
        "error_type": "TYPE_ERROR",
        "error_reason": "Invalid float field: Humidity_pct='abc_xyz'",
        "raw_row": {
            "Location": random.choice(LOCATIONS),
            "Date_Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Temperature_C": str(round(random.uniform(10, 35), 5)),
            "Humidity_pct": "abc_xyz",
            "Precipitation_mm": str(round(random.uniform(0, 20), 6)),
            "Wind_Speed_kmh": str(round(random.uniform(0, 10), 3)),
            "retry": 0
        },
        "timestamp": time.time(),
        "file_name": "stress_error_test.csv",
        "retry_count": 0
    }


def generate_error():
    return generate_error_missing_field() if random.random() < 0.5 else generate_error_type_error()


# ------------------------------------------------
# topic router
# ------------------------------------------------
def generate_message(topic):
    if topic == "weather-data":
        return generate_weather_like(retry=False)
    if topic == "retry-data":
        return generate_weather_like(retry=True)
    if topic == "error-data":
        return generate_error()
    raise ValueError(f"Unknown topic: {topic}")


# ------------------------------------------------
# worker: TPS 제한 + 무한 루프
# ------------------------------------------------
def worker(thread_id, topic, tps_per_thread):
    producer = get_producer()
    print(f"🧵 Thread-{thread_id} started | Target TPS={tps_per_thread}")

    while True:
        start_sec = time.time()
        sent_this_sec = 0

        while sent_this_sec < tps_per_thread:
            msg = generate_message(topic)
            key = msg.get("Location", "default")
            producer.send(topic, key=key, value=msg)
            sent_this_sec += 1

        producer.flush()

        elapsed = time.time() - start_sec
        if elapsed < 1:
            time.sleep(1 - elapsed)


# ------------------------------------------------
# main
# ------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Kafka Continuous TPS Stress Tester")

    parser.add_argument("-t", "--topic", required=True, help="Topic name")
    parser.add_argument("-T", "--threads", required=True, type=int, help="Thread count")
    parser.add_argument("-r", "--rate", required=True, type=int, help="Messages per second (global)")

    args = parser.parse_args()

    topic = args.topic
    threads = args.threads
    rate = args.rate

    # thread당 TPS 계산
    tps_per_thread = rate // threads

    print(f"\n🔥 Continuous Stress Test Started")
    print(f"   Topic: {topic}")
    print(f"   Threads: {threads}")
    print(f"   Global TPS: {rate}")
    print(f"   TPS per thread: {tps_per_thread}\n")
    print("🔁 Running infinitely... Press CTRL+C to stop.\n")

    thread_list = []
    for i in range(threads):
        t = threading.Thread(target=worker, args=(i, topic, tps_per_thread))
        t.daemon = True
        t.start()
        thread_list.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping stress test...")


if __name__ == "__main__":
    main()
