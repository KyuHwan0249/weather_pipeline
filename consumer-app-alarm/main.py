import os
import json
import requests
from datetime import datetime
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, collect_list, struct
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

from kafka import KafkaProducer
from db.alert_repository import save_alert, update_alert_sent


###########################################
# CONFIG
###########################################
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPIC_NAME = os.getenv("TOPIC_WEATHER")
RETRY_TOPIC = os.getenv("TOPIC_RETRY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS"))
ALERT_INTERVAL_MINUTES = int(os.getenv("ALERT_INTERVAL_MINUTES"))
WATER_MARK_MINUTES = int(os.getenv("WATERMARK_MINUTES"))
HIGH_TEMPERATURE_THRESHOLD = float(os.getenv("HIGH_TEMPERATURE_THRESHOLD"))
LOW_TEMPERATURE_THRESHOLD = float(os.getenv("LOW_TEMPERATURE_THRESHOLD"))
RAINFALL_THRESHOLD = float(os.getenv("RAINFALL_THRESHOLD"))
WIND_SPEED_THRESHOLD = float(os.getenv("WIND_SPEED_THRESHOLD"))
RANDOM_LIMIT = float(os.getenv("RANDOM_LIMIT", "0.2"))

###########################################
# 상태 저장 (event_time 기반)
###########################################
last_alert_time = {}

def should_alert(location, alert_type, event_time):
    key = (location, alert_type)

    if key in last_alert_time:
        diff = event_time - last_alert_time[key]
        if diff.total_seconds() < ALERT_INTERVAL_MINUTES * 60:
            return False

    last_alert_time[key] = event_time
    return True


###########################################
# Slack
###########################################
def send_slack(payload):
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": payload})
        return res.status_code == 200
    except:
        return False

retry_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    key_serializer=lambda v: v.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

###########################################
# Spark Session
###########################################
spark = (
    SparkSession.builder
        .appName("WeatherAlertConsumer")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


###########################################
# Schema
###########################################
schema = StructType([
    StructField("Location", StringType()),
    StructField("Date_Time", StringType()),
    StructField("Temperature_C", DoubleType()),
    StructField("Humidity_pct", DoubleType()),
    StructField("Precipitation_mm", DoubleType()),
    StructField("Wind_Speed_kmh", DoubleType()),
    StructField("retry", DoubleType())
])


###########################################
# Kafka → Parsed Stream
###########################################
raw_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")  # 혹은 earliest
        # ▼▼▼ [추가] 타임아웃 설정 늘리기 ▼▼▼
        .option("kafka.request.timeout.ms", "60000")      # 요청 타임아웃 60초 (기본 30초)
        .option("kafka.session.timeout.ms", "60000")      # 세션 타임아웃 60초 (기본 10초)
        .option("kafka.connection.timeout.ms", "60000")   # 연결 타임아웃 60초
        .option("retries", "5")                           # 실패 시 5번까지 재시도
        .option("maxOffsetsPerTrigger", 10000)
        .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
          .withColumn("event_time", to_timestamp(col("Date_Time")))
)

###########################################
# Event-time Window 1분 + Watermark 2분
###########################################
windowed_df = (
    parsed_df
        .withWatermark("event_time", f"{WATER_MARK_MINUTES} minutes")
        .groupBy(
            window(col("event_time"), f"{WINDOW_SECONDS} seconds"),
            col("Location")
        )
        .agg(collect_list(struct("*")).alias("rows"))
)


###########################################
# Alert Logic
###########################################
def detect_alert_types(row):
    alerts = []
    t = row["Temperature_C"]
    p = row["Precipitation_mm"]
    w = row["Wind_Speed_kmh"]

    if t is not None and t >= HIGH_TEMPERATURE_THRESHOLD:
        alerts.append(("TEMP_HIGH", f"Temperature {t}°C >= {HIGH_TEMPERATURE_THRESHOLD}°C", t, HIGH_TEMPERATURE_THRESHOLD))
    if t is not None and t <= LOW_TEMPERATURE_THRESHOLD:
        alerts.append(("TEMP_LOW", f"Temperature {t}°C <= {LOW_TEMPERATURE_THRESHOLD}°C", t, LOW_TEMPERATURE_THRESHOLD))
    if p is not None and p >= RAINFALL_THRESHOLD:
        alerts.append(("RAIN_HEAVY", f"Rainfall {p}mm >= {RAINFALL_THRESHOLD}mm", p, RAINFALL_THRESHOLD))
    if w is not None and w >= WIND_SPEED_THRESHOLD:
        alerts.append(("WIND_STRONG", f"Wind {w} km/h >= {WIND_SPEED_THRESHOLD} km/h", w, WIND_SPEED_THRESHOLD))

    return alerts


###########################################
# foreachBatch (Window 단위 처리)
###########################################
def process_window_batch(df, batch_id):
    rows = df.collect()
    if not rows:
        print(f"[BATCH {batch_id}] No rows")
        return

    for r in rows:
        loc = r["Location"]
        window_start = r["window"]["start"]
        window_end = r["window"]["end"]
        record_list = r["rows"]

        for raw_row in record_list:
            raw_dict = raw_row.asDict()
            event_time = raw_dict["event_time"]

            # 강제 재시도 메시지 전송
            if random.random() < RANDOM_LIMIT:
                # ⭐ datetime → string 변환
                safe_dict = dict(raw_dict)
                if isinstance(safe_dict.get("event_time"), datetime):
                    safe_dict["event_time"] = safe_dict["event_time"].isoformat()

                retry_producer.send(
                    RETRY_TOPIC,
                    key=loc,
                    value=safe_dict
                )
                # flush 제거 권장 (executor block 방지)
                print(f"⚠️ Forced Retry → retry-topic: {loc}")
                continue
            
            # Alert 체크
            triggered = detect_alert_types(raw_dict)

            for alert_type, reason, value, threshold in triggered:

                # event_time 기반 쿨다운
                if not should_alert(loc, alert_type, event_time):
                    continue

                # DB 저장
                alert_id = save_alert(
                    location=loc,
                    alert_type=alert_type,
                    alert_reason=reason,
                    event_time=event_time,
                    value=value,
                    threshold=threshold,
                    raw_row=raw_dict,
                    slack_sent=False
                )

                emoji = {
                    "TEMP_HIGH": "🔥",
                    "TEMP_LOW": "❄️",
                    "RAIN_HEAVY": "🌧️",
                    "WIND_STRONG": "💨",
                }.get(alert_type, "⚠️")

                # Slack Payload
                payload = (
                    f"{emoji} *{alert_type.replace('_', ' ')} Alert*\n"
                    f"Location: {loc}\n"
                    f"{reason}\n"
                    f"Event Time: {event_time}\n"
                    f"Window: {window_start} ~ {window_end}"
                )

                success = send_slack(payload)

                if success:
                    update_alert_sent(alert_id)
                    print(f"🚨 Alert sent + updated: {loc} {alert_type}")
                else:
                    print(f"⚠ Slack failed: {loc} {alert_type}")


###########################################
# Streaming 실행
###########################################
query = (
    windowed_df.writeStream
        .foreachBatch(process_window_batch)
        .outputMode("update")
        .option("checkpointLocation", "/shared-checkpoints/weather-alert")
        .start()
)

spark.streams.awaitAnyTermination()
