import os
import json
import requests
import random
from datetime import datetime

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


###########################################################
# CONFIG
###########################################################
def load_config():
    return {
        "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP"),
        "TOPIC_NAME": os.getenv("TOPIC_WEATHER"),
        "RETRY_TOPIC": os.getenv("TOPIC_RETRY"),
        "SLACK_WEBHOOK_URL": os.getenv("SLACK_WEBHOOK_URL"),

        "WINDOW_SECONDS": int(os.getenv("WINDOW_SECONDS", "60")),
        "ALERT_INTERVAL_MINUTES": int(os.getenv("ALERT_INTERVAL_MINUTES", "30")),
        "WATER_MARK_MINUTES": int(os.getenv("WATERMARK_MINUTES", "2")),
        "HIGH_TEMPERATURE_THRESHOLD": float(os.getenv("HIGH_TEMPERATURE_THRESHOLD", "35")),
        "LOW_TEMPERATURE_THRESHOLD": float(os.getenv("LOW_TEMPERATURE_THRESHOLD", "0")),
        "RAINFALL_THRESHOLD": float(os.getenv("RAINFALL_THRESHOLD", "30")),
        "WIND_SPEED_THRESHOLD": float(os.getenv("WIND_SPEED_THRESHOLD", "40")),
        "RANDOM_LIMIT": float(os.getenv("RANDOM_LIMIT", "0.2")),
        # 성능/안정성 옵션 (원하면 ENV로 꺼낼 수 있음)
        "MAX_OFFSETS_PER_TRIGGER": int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "2000")),
        "CHECKPOINT_LOCATION": os.getenv("CHECKPOINT_LOCATION", "/shared-checkpoints/weather-alert"),
    }


###########################################################
# ALERT STATE (쿨다운)
###########################################################
last_alert_time = {}


def should_alert(location, alert_type, event_time, cooldown_min):
    """
    같은 (location, alert_type)에 대해 일정 시간 내 중복 알림 방지
    """
    key = (location, alert_type)

    if key in last_alert_time:
        diff = (event_time - last_alert_time[key]).total_seconds()
        if diff < cooldown_min * 60:
            return False

    last_alert_time[key] = event_time
    return True


###########################################################
# Slack
###########################################################
def send_slack(webhook_url, payload: str) -> bool:
    try:
        res = requests.post(webhook_url, json={"text": payload}, timeout=5)
        return res.status_code == 200
    except Exception as e:
        print(f"[SLACK] Error: {e}")
        return False


###########################################################
# Schema
###########################################################
def get_schema():
    return StructType([
        StructField("Location", StringType()),
        StructField("Date_Time", StringType()),
        StructField("Temperature_C", DoubleType()),
        StructField("Humidity_pct", DoubleType()),
        StructField("Precipitation_mm", DoubleType()),
        StructField("Wind_Speed_kmh", DoubleType()),
        StructField("retry", DoubleType()),
    ])


###########################################################
# Alert Logic
###########################################################
def detect_alert_types(row, cfg):
    alerts = []

    t = row.get("Temperature_C")
    p = row.get("Precipitation_mm")
    w = row.get("Wind_Speed_kmh")

    if t is not None and t >= cfg["HIGH_TEMPERATURE_THRESHOLD"]:
        alerts.append((
            "TEMP_HIGH",
            f"Temperature {t}°C >= {cfg['HIGH_TEMPERATURE_THRESHOLD']}°C",
            t,
            cfg["HIGH_TEMPERATURE_THRESHOLD"],
        ))

    if t is not None and t <= cfg["LOW_TEMPERATURE_THRESHOLD"]:
        alerts.append((
            "TEMP_LOW",
            f"Temperature {t}°C <= {cfg['LOW_TEMPERATURE_THRESHOLD']}°C",
            t,
            cfg["LOW_TEMPERATURE_THRESHOLD"],
        ))

    if p is not None and p >= cfg["RAINFALL_THRESHOLD"]:
        alerts.append((
            "RAIN_HEAVY",
            f"Rainfall {p}mm >= {cfg['RAINFALL_THRESHOLD']}mm",
            p,
            cfg["RAINFALL_THRESHOLD"],
        ))

    if w is not None and w >= cfg["WIND_SPEED_THRESHOLD"]:
        alerts.append((
            "WIND_STRONG",
            f"Wind {w} km/h >= {cfg['WIND_SPEED_THRESHOLD']} km/h",
            w,
            cfg["WIND_SPEED_THRESHOLD"],
        ))

    return alerts


###########################################################
# foreachBatch 처리 함수 생성 (클로저)
###########################################################
def create_batch_processor(cfg, retry_producer):

    def process_window_batch(df, batch_id):
        """
        ⚠ 중요: df.collect() 사용하지 않고, toLocalIterator()로 스트리밍하게 처리
        """
        print(f"[BATCH {batch_id}] Starting batch processing...")

        # df: columns = [window, Location, rows]
        # rows: array<struct<원본 컬럼들 + event_time>>
        count = 0

        try:
            for grouped_row in df.toLocalIterator():
                loc = grouped_row["Location"]
                window_struct = grouped_row["window"]
                window_start = window_struct["start"]
                window_end = window_struct["end"]
                record_list = grouped_row["rows"]  # 이게 이미 array<struct> (배열)

                for raw_row in record_list:
                    raw_dict = raw_row.asDict(recursive=True)
                    event_time = raw_dict.get("event_time")

                    # event_time이 string이면 datetime으로 변환
                    if isinstance(event_time, str):
                        try:
                            event_time = datetime.fromisoformat(event_time)
                            raw_dict["event_time"] = event_time
                        except Exception:
                            # 파싱 실패 시 그냥 스킵
                            continue

                    # 강제 retry 샘플링
                    if random.random() < cfg["RANDOM_LIMIT"]:
                        safe_dict = dict(raw_dict)
                        if isinstance(safe_dict.get("event_time"), datetime):
                            safe_dict["event_time"] = safe_dict["event_time"].isoformat()

                        retry_producer.send(
                            cfg["RETRY_TOPIC"],
                            key=loc,
                            value=safe_dict,
                        )
                        print(f"⚠ Forced retry → {cfg['RETRY_TOPIC']} / loc={loc}")
                        continue

                    # Alert 조건 검사
                    alerts = detect_alert_types(raw_dict, cfg)

                    for alert_type, reason, value, threshold in alerts:
                        if not should_alert(
                            loc,
                            alert_type,
                            event_time,
                            cfg["ALERT_INTERVAL_MINUTES"],
                        ):
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
                            slack_sent=False,
                        )

                        emoji = {
                            "TEMP_HIGH": "🔥",
                            "TEMP_LOW": "❄️",
                            "RAIN_HEAVY": "🌧️",
                            "WIND_STRONG": "💨",
                        }.get(alert_type, "⚠️")

                        payload = (
                            f"{emoji} *{alert_type.replace('_', ' ')} Alert*\n"
                            f"Location: {loc}\n"
                            f"{reason}\n"
                            f"Event Time: {event_time}\n"
                            f"Window: {window_start} ~ {window_end}"
                        )

                        success = send_slack(cfg["SLACK_WEBHOOK_URL"], payload)

                        if success:
                            update_alert_sent(alert_id)
                            print(f"🚨 Alert sent & updated: loc={loc}, type={alert_type}")
                        else:
                            print(f"⚠ Slack failed: loc={loc}, type={alert_type}")

                        count += 1

            print(f"[BATCH {batch_id}] Done. Alerts processed: {count}")

        except Exception as e:
            # 배치 전체가 죽어도 스트리밍은 계속 돌아가도록 로그만 찍기
            print(f"[BATCH {batch_id}] ERROR: {e}")

    return process_window_batch


###########################################################
# MAIN
###########################################################
def main():
    cfg = load_config()
    print("[INIT] Loaded config:", cfg)

    # Kafka Producer (late init)
    retry_producer = KafkaProducer(
        bootstrap_servers=cfg["KAFKA_BOOTSTRAP"].split(","),
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("WeatherAlertConsumer")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        )
        # executor/네트워크 안정성 옵션 (원하면 더 추가)
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeout", "300s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = get_schema()

    # Kafka에서 raw read
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["KAFKA_BOOTSTRAP"])
        .option("subscribe", cfg["TOPIC_NAME"])
        .option("startingOffsets", "latest")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.connection.timeout.ms", "60000")
        .option("maxOffsetsPerTrigger", cfg["MAX_OFFSETS_PER_TRIGGER"])
        .load()
    )

    # JSON 파싱 + event_time 컬럼 추가
    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("Date_Time")))
    )

    # 윈도우 + 워터마크
    windowed_df = (
        parsed_df
        .withWatermark("event_time", f"{cfg['WATER_MARK_MINUTES']} minutes")
        .groupBy(
            window(col("event_time"), f"{cfg['WINDOW_SECONDS']} seconds"),
            col("Location"),
        )
        .agg(collect_list(struct("*")).alias("rows"))
    )

    process_fn = create_batch_processor(cfg, retry_producer)

    query = (
        windowed_df.writeStream
        .foreachBatch(process_fn)
        .outputMode("update")
        .option("checkpointLocation", cfg["CHECKPOINT_LOCATION"])
        .start()
    )

    print("[STREAM] WeatherAlertConsumer started.")
    spark.streams.awaitAnyTermination()


###########################################################
# ENTRYPOINT
###########################################################
if __name__ == "__main__":
    main()
