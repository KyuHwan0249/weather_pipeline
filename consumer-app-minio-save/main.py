import json
import io
import time
import boto3
import pandas as pd
from kafka import KafkaConsumer

TOPIC_NAME = "weather-data"
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
BUCKET_NAME = "weather-bucket"

# ✅ MinIO 설정
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1"
)

# ✅ Kafka Consumer 설정
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="origin-consumer-group",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="earliest"
)

print("🚀 Consumer started. Listening for messages...")

# ✅ location별 버퍼 관리
buffer_by_location = {}
last_flush_time = {}
BATCH_SIZE = 50
FLUSH_INTERVAL = 30  # 초 단위

def flush_to_minio(location):
    """해당 location 버퍼를 parquet으로 저장"""
    if not buffer_by_location.get(location):
        return

    df = pd.DataFrame(buffer_by_location[location])
    buffer_by_location[location].clear()

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    key = f"location={location}/{timestamp}.parquet"

    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
    print(f"✅ Flushed {len(df)} records to MinIO → {key}")

    last_flush_time[location] = time.time()

# ======================================================
# 메인 루프 (poll 기반)
# ======================================================
while True:
    try:
        # poll() 은 일정 시간(1초) 동안 메시지 기다림
        records = consumer.poll(timeout_ms=1000)

        now = time.time()

        # 🔹 메시지 수신 시
        for tp, messages in records.items():
            for message in messages:
                location = message.key or "unknown"
                value = message.value

                buffer_by_location.setdefault(location, []).append(value)
                last_flush_time.setdefault(location, now)

                # ① 개수 조건
                if len(buffer_by_location[location]) >= BATCH_SIZE:
                    flush_to_minio(location)

        # 🔹 시간 조건 (메시지 없어도 체크 가능)
        for location, last_time in last_flush_time.items():
            if now - last_time >= FLUSH_INTERVAL:
                flush_to_minio(location)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(3)
