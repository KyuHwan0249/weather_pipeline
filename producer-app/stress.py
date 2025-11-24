import json
import time
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

# 컨테이너 내부 통신이므로 서비스명(kafka-1) 사용
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
TOPIC_NAME = "weather-data"
TOTAL_MESSAGES = 1000000
NUM_THREADS = 25

LOCATIONS = ["Seoul", "New York", "London", "Tokyo", "Paris", "Busan", "Jeju"]

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=32768
    )

def generate_weather_data():
    now = datetime.now().isoformat()
    location = random.choice(LOCATIONS)
    
    # 50% 확률로 이상치 생성 (알람/에러 유발)
    if random.random() < 0.5:
        temp = random.choice([35.5, -15.0]) 
        precip = random.choice([0.0, 50.5]) 
    else:
        temp = round(random.uniform(10, 25), 2)
        precip = 0.0

    return {
        "Location": location,
        "Date_Time": now,
        "Temperature_C": temp,
        "Humidity_pct": round(random.uniform(30, 90), 1),
        "Precipitation_mm": precip,
        "Wind_Speed_kmh": round(random.uniform(0, 15), 1),
        "retry": 0
    }

def send_messages(thread_id, count):
    producer = get_producer()
    print(f"🧵 Thread-{thread_id} started")
    
    for i in range(count):
        data = generate_weather_data()
        producer.send(TOPIC_NAME, key=data["Location"], value=data)
        # 속도 제한 없이 최대한 빠르게 전송
        
    producer.flush()
    print(f"✅ Thread-{thread_id} finished")

def main():
    print(f"🔥 Starting Attack: {TOTAL_MESSAGES} msgs")
    threads = []
    msgs_per_thread = TOTAL_MESSAGES // NUM_THREADS
    
    start = time.time()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=send_messages, args=(i, msgs_per_thread))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()
        
    end = time.time()
    print(f"🚀 Done in {end - start:.2f} sec (Throughput: {TOTAL_MESSAGES / (end - start):.0f} msg/s)")

if __name__ == "__main__":
    main()
