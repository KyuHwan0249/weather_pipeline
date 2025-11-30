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
TOPIC_NAME = os.getenv("TOPIC_WEATHER")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP")
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
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id="origin-consumer-group-final",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="latest"
    )


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
        dt_first = datetime.fromisoformat(df.iloc[0]["Date_Time"])
    except ValueError:
        dt_first = datetime.strptime(df.iloc[0]["Date_Time"], "%Y-%m-%d %H:%M:%S")

    ts_name = dt_first.strftime("%Y%m%d_%H%M%S")
    uuid_suffix = uuid.uuid4().hex[:6]
    file_name = f"{ts_name}_{uuid_suffix}.parquet"
    key = f"location={location}/date={date}/{file_name}"

    buffer_io = io.BytesIO()
    df.to_parquet(buffer_io, index=False)
    buffer_io.seek(0)

    minio_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=buffer_io.getvalue()
    )

    print(f"📤 FLUSHED → {key}  (rows={len(df)})")
    last_flush[(location, date)] = time.time()


#############################################
# Single message processing
#############################################
def process_message(val, location):
    """Process a single kafka record and add to buffer."""
    now = time.time()

    # Parse datetime safely
    try:
        dt = datetime.fromisoformat(val["Date_Time"])
    except ValueError:
        dt = datetime.now()

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

    consumer = create_consumer()
    minio_client = create_minio_client()

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    val = msg.value
                    location = msg.key or "unknown"

                    flush_target = process_message(val, location)

                    if flush_target:
                        loc, date = flush_target
                        flush(minio_client, loc, date)

            # periodic flush
            flush_due(minio_client)

        except Exception as e:
            print(f"❌ ERROR: {e}")
            time.sleep(1)


#############################################
# MAIN ENTRYPOINT
#############################################
if __name__ == "__main__":
    run_consumer()
