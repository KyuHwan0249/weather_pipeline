
import os

import json
import requests
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

from db.alert_repository import save_alert, update_alert_sent


ALERT_EMOJI = {
    "TEMP_HIGH": "🔥",
    "TEMP_LOW": "❄️",
    "RAIN_HEAVY": "🌧️",
    "WIND_STRONG": "💨",
    "DEFAULT": "⚠️"
}

###########################################
# CONFIG
###########################################
KAFKA_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
TOPIC_NAME = "weather-data"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

STREAM_INTERVAL_SECONDS = 5
ALERT_INTERVAL_MINUTES = 10   # 10분 룰

###########################################
# Slack
###########################################
def send_slack(message):
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        print("📨 Slack alert sent")
    except Exception as e:
        print("❌ Slack error:", e)

###########################################
# 상태 저장
###########################################
# Key = (location, alert_type)
last_alert_time = {}

###########################################
# 중복 알람 방지 체크
###########################################
def should_alert(location, alert_type):
    now = datetime.now()
    key = (location, alert_type)

    if key in last_alert_time:
        diff = now - last_alert_time[key]
        if diff.total_seconds() < ALERT_INTERVAL_MINUTES * 60:
            return False

    last_alert_time[key] = now
    return True


###########################################
# Spark Session
###########################################
spark = (
    SparkSession.builder
        .appName("WeatherAlertConsumer")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")
        .config("spark.executorEnv.USER", "sparkuser")
        .config("spark.driver.extraJavaOptions", "-Duser.name=sparkuser")
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
# Kafka Stream
###########################################
raw_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
)

###########################################
# Alert 조건별 식별
###########################################
def detect_alert_types(row):
    alerts = []

    t = row["Temperature_C"]
    p = row["Precipitation_mm"]
    w = row["Wind_Speed_kmh"]

    # 온도 높은 경우
    if t is not None and t >= 28:
        alerts.append((
            "TEMP_HIGH",
            f"Temperature {t}°C >= 28°C",
            t,      # value
            28.0    # threshold
        ))

    # 온도 낮은 경우
    if t is not None and t <= -5:
        alerts.append((
            "TEMP_LOW",
            f"Temperature {t}°C <= -5°C",
            t,
            -5.0
        ))

    # 강수량
    if p is not None and p >= 8:
        alerts.append((
            "RAIN_HEAVY",
            f"Rainfall {p}mm >= 8mm",
            p,
            8.0
        ))

    # 풍속
    if w is not None and w >= 25:
        alerts.append((
            "WIND_STRONG",
            f"Wind {w} km/h >= 25 km/h",
            w,
            25.0
        ))

    return alerts


###########################################
# foreachBatch
###########################################
def process_batch(df, batch_id):
    rows = df.collect()
    if not rows:
        print(f"[BATCH {batch_id}] No rows")
        return

    for r in rows:
        loc = r["Location"]
        ts = r["Date_Time"]

        triggered_alerts = detect_alert_types(r)

        for alert_type, reason, value, threshold in triggered_alerts:

            # 🔥 쿨다운이면 아무것도 하지 않음 (DB 저장 X)
            if not should_alert(loc, alert_type):
                print(f"🔁 Skipped (cooldown): {loc} / {alert_type}")
                continue

            alert_id = save_alert(
                location=loc,
                alert_type=alert_type,
                alert_reason=reason,
                event_time=ts,
                value=value,
                threshold=threshold,
                raw_row=r.asDict(),  # 전체 row를 JSONB로 저장
                slack_sent=False
            )

            print(f"📝 DB inserted alert_id={alert_id} / {loc} / {alert_type}")

            # ---------------------------
            # 2️⃣ Slack 전송
            # ---------------------------
            emoji = ALERT_EMOJI.get(alert_type, ALERT_EMOJI["DEFAULT"])
            success = False

            try:
                res = requests.post(SLACK_WEBHOOK_URL, json={"text": 
                    f"{emoji} *{alert_type.replace('_', ' ')} Alert*\n"
                    f"Location: {loc}\n"
                    f"{reason}\n"
                    f"Time: {ts}"
                })
                success = (res.status_code == 200)
            except Exception as e:
                print(f"❌ Slack send error: {e}")
                success = False

            # ---------------------------
            # 3️⃣ Slack 성공 → DB UPDATE
            # ---------------------------
            if success:
                update_alert_sent(alert_id)
                print(f"🚨 Alert sent + DB updated: {loc} / {alert_type}")
            else:
                print(f"⚠️ Slack FAILED (DB remains unsent): {loc} / {alert_type}")

###########################################
# Streaming 실행
###########################################
query = (
    parsed_df.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=f"{STREAM_INTERVAL_SECONDS} seconds")
        .outputMode("update")
        .option("checkpointLocation", "/app/checkpoints/weather-condition-alert")
        .start()
)

spark.streams.awaitAnyTermination()
