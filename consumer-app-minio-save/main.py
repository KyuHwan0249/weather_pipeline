import os
import json
import io
import time
import uuid
import boto3
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

#############################################
# CONFIG & GLOBALS
#############################################
# 환경변수가 없으면 기본값으로 'weather-data'를 쓰도록 수정 (안전장치)
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-data") 
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "weather-bucket")

BATCH_SIZE = 2000
FLUSH_INTERVAL = 60   # seconds

# in-memory buffers
buffer = {}
last_flush = {}

#############################################
# MinIO client
#############################################
def create_minio_client():
    print(f"🔌 Connecting to MinIO at {os.getenv('MINIO_ENDPOINT')}...")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1"
    )

#############################################
# Kafka Consumer
#############################################
def create_consumer():
    print(f"🔌 Connecting to Kafka Brokers: {BOOTSTRAP_SERVERS}")
    print(f"🎯 Target Topic: {TOPIC_NAME}")  # <--- 여기가 None이면 로그에 찍힘
    print(f"👥 Consumer Group: origin-consumer-group-final")

    if not TOPIC_NAME:
        raise ValueError("❌ ERROR: TOPIC_NAME is empty! Check docker-compose environment variables.")

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id="origin-consumer-group-final",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        # ▼▼▼ 로드테스트 때는 이걸 earliest로 해야 놓친 데이터를 다 가져옵니다 ▼▼▼
        auto_offset_reset="earliest" 
    )
    
    # 명시적으로 구독 선언 (가장 확실함)
    consumer.subscribe([TOPIC_NAME])
    print(f"✅ Successfully Subscribed to [{TOPIC_NAME}]")
    
    return consumer

#############################################
# Flush function
#############################################
def flush(minio_client, location, date):
    """Flush buffered rows to MinIO as parquet."""
    if location not in buffer or date not in buffer[location]:
        return

    rows = buffer[location][date]
    if not rows:
        return

    df = pd.DataFrame(rows)
    buffer[location][date] = []

    # Parse date safely
    try:
        # Kafka 메시지에 Date_Time이 있다면 사용
        dt_val = df.iloc[0].get("Date_Time")
        if dt_val:
            dt_first = datetime.fromisoformat(dt_val)
        else:
            dt_first = datetime.now()
    except Exception:
        dt_first = datetime.now()

    ts_name = dt_first.strftime("%Y%m%d_%H%M%S")
    uuid_suffix = uuid.uuid4().hex[:6]
    file_name = f"{ts_name}_{uuid_suffix}.parquet"
    key = f"location={location}/date={date}/{file_name}"

    buffer_io = io.BytesIO()
    df.to_parquet(buffer_io, index=False)
    buffer_io.seek(0)

    try:
        minio_client.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=buffer_io.getvalue()
        )
        print(f"📤 FLUSHED → {key}  (rows={len(df)})")
    except Exception as e:
        print(f"❌ MinIO Upload Failed: {e}")

    last_flush[(location, date)] = time.time()

#############################################
# Single message processing
#############################################
def process_message(val, location):
    """Process a single kafka record and add to buffer."""
    now = time.time()

    # [수정] 날짜 형식이 이상하면 데이터를 버립니다 (Skip)
    try:
        dt_str = val.get("Date_Time")
        # 정확히 포맷이 일치하는지 확인 ("2024-01-01 00:00:00")
        dt = datetime.strptime(str(dt_str), "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # 형식이 안 맞으면 None을 리턴하여 버퍼에 추가하지 않음
        # (필요하다면 로그를 찍어서 확인)
        # print(f"⚠️ [SKIP] Invalid Date Format: {val.get('Date_Time')} | Data: {val}")
        return None

    # 날짜가 정상이면 문자열로 변환 (폴더 경로용)
    date_str = dt.strftime("%Y-%m-%d")

    buffer.setdefault(location, {})
    buffer[location].setdefault(date_str, [])
    buffer[location][date_str].append(val)

    # set initial flush timestamp
    last_flush.setdefault((location, date_str), now)

    # batch flush
    if len(buffer[location][date_str]) >= BATCH_SIZE:
        return location, date_str

    return None

#############################################
# Periodic flush logic
#############################################
def flush_due(minio_client):
    now = time.time()
    for (loc, d), last_time in list(last_flush.items()):
        if now - last_time >= FLUSH_INTERVAL:
            flush(minio_client, loc, d)

#############################################
# Main consumer loop
#############################################
def run_consumer():
    print("🚀 Kafka Consumer Started (Function-based mode)\n")

    # 여기서 에러가 나면 바로 로그에 찍힘
    try:
        consumer = create_consumer()
        minio_client = create_minio_client()
    except Exception as e:
        print(f"❌ FATAL ERROR during initialization: {e}")
        return

    print("👂 Waiting for messages...")

    while True:
        try:
            # 타임아웃을 1초로 줘서 루프가 계속 돌게 함
            records = consumer.poll(timeout_ms=1000)
            
            # 메시지가 없어도 루프는 돔 (여기서 주기적 flush 체크)
            if not records:
                flush_due(minio_client)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    val = msg.value
                    location = msg.key or "unknown"

                    # print(f"Processing: {location}") # 디버깅용 (너무 많으면 주석)
                    flush_target = process_message(val, location)

                    if flush_target:
                        loc, date = flush_target
                        flush(minio_client, loc, date)

            # periodic flush
            flush_due(minio_client)

        except Exception as e:
            print(f"❌ ERROR inside loop: {e}")
            time.sleep(1)

#############################################
# MAIN ENTRYPOINT
#############################################
if __name__ == "__main__":
    run_consumer()