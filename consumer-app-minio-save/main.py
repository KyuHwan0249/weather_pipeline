import json
import io
import time
import uuid
import boto3
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

TOPIC_NAME = "weather-data"
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
BUCKET_NAME = "weather-bucket"

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1"
)

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="origin-consumer-group",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="latest"
)

print("🚀 Consumer started...\n")

# ==================================================
#   (location → date → buffer) 구조
# ==================================================
buffer = {}              # buffer[location][date] = []
last_flush = {}          # last_flush[(location, date)] = timestamp

BATCH_SIZE = 50
FLUSH_INTERVAL = 30   # seconds


# ==================================================
#   flush 함수 (정리된 print 버전)
# ==================================================
def flush(location, date):
    if location not in buffer or date not in buffer[location]:
        return

    rows = buffer[location][date]
    if not rows:
        return

    df = pd.DataFrame(rows)
    buffer[location][date] = []

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


# ==================================================
# 🔥 main loop (불필요한 출력 제거)
# ==================================================
while True:
    try:
        records = consumer.poll(timeout_ms=1000)
        now = time.time()

        # 메시지가 있을 때만 출력
        for tp, messages in records.items():
            for msg in messages:
                val = msg.value
                location = msg.key or "unknown"

                dt = datetime.strptime(val["Date_Time"], "%Y-%m-%d %H:%M:%S")
                date_str = dt.strftime("%Y-%m-%d")

                # 메시지 도착 시 최소 정보만 출력
                print(f"📩 msg: location={location}, date={date_str}")

                # 버퍼 저장
                buffer.setdefault(location, {})
                buffer[location].setdefault(date_str, [])
                buffer[location][date_str].append(val)

                last_flush.setdefault((location, date_str), now)

                # 개수 조건
                if len(buffer[location][date_str]) >= BATCH_SIZE:
                    flush(location, date_str)

        # 시간 조건 flush
        for (loc, d), last_time in list(last_flush.items()):
            if now - last_time >= FLUSH_INTERVAL:
                flush(loc, d)

        # 날짜 변경 시 flush
        today = datetime.now().strftime("%Y-%m-%d")
        for loc in list(buffer.keys()):
            for d in list(buffer[loc].keys()):
                if d != today:
                    flush(loc, d)
                    buffer[loc].pop(d, None)

    except Exception as e:
        print(f"❌ ERROR: {e}")
        time.sleep(3)
