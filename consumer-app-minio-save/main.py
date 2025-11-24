import os
import json
import io
import time
import uuid
import boto3
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

TOPIC_NAME = os.getenv("TOPIC_WEATHER")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "weather-bucket")

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    region_name="us-east-1"
)

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
    group_id="origin-consumer-group-final",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="latest" # 과거 데이터 필수
)

print("🚀 Consumer started (Fixed Date Format)...\n")

buffer = {}
last_flush = {}

# 배치 사이즈를 넉넉하게 잡아야 S3 업로드 병목이 안 생김
BATCH_SIZE = 2000 
FLUSH_INTERVAL = 60

def flush(location, date):
    if location not in buffer or date not in buffer[location]:
        return

    rows = buffer[location][date]
    if not rows:
        return

    df = pd.DataFrame(rows)
    buffer[location][date] = []

    # 🔥 [수정 1] ISO 포맷 자동 인식
    try:
        dt_first = datetime.fromisoformat(df.iloc[0]["Date_Time"])
    except ValueError:
        # 혹시라도 예전 포맷이 섞여있을 경우를 대비한 안전장치
        dt_first = datetime.strptime(df.iloc[0]["Date_Time"], "%Y-%m-%d %H:%M:%S")
        
    ts_name = dt_first.strftime("%Y%m%d_%H%M%S")
    uuid_suffix = uuid.uuid4().hex[:6]
    file_name = f"{ts_name}_{uuid_suffix}.parquet"
    key = f"location={location}/date={date}/{file_name}"

    buffer_io = io.BytesIO()
    df.to_parquet(buffer_io, index=False)
    buffer_io.seek(0)

    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer_io.getvalue())

    print(f"📤 FLUSHED → {key}  (rows={len(df)})")
    last_flush[(location, date)] = time.time()


while True:
    try:
        records = consumer.poll(timeout_ms=1000)
        now = time.time()

        for tp, messages in records.items():
            for msg in messages:
                val = msg.value
                location = msg.key or "unknown"

                # 🔥 [수정 2] ISO 포맷 자동 인식
                try:
                    dt = datetime.fromisoformat(val["Date_Time"])
                except ValueError:
                    # 포맷 에러 시 그냥 현재 시간으로 처리하거나 스킵
                    dt = datetime.now()
                
                date_str = dt.strftime("%Y-%m-%d")

                # 로그는 너무 많으니 1000개 단위로 찍거나 생략 추천
                # print(f"📩 msg: location={location}, date={date_str}")

                buffer.setdefault(location, {})
                buffer[location].setdefault(date_str, [])
                buffer[location][date_str].append(val)
                last_flush.setdefault((location, date_str), now)

                if len(buffer[location][date_str]) >= BATCH_SIZE:
                    flush(location, date_str)

        # 시간 조건 flush
        for (loc, d), last_time in list(last_flush.items()):
            if now - last_time >= FLUSH_INTERVAL:
                flush(loc, d)

    except Exception as e:
        print(f"❌ ERROR: {e}")
        time.sleep(1)