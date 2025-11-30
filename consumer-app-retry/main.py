import os
import json
import time
import random
import requests
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# DB 저장 함수
from db.alert_repository import save_alert, update_alert_sent


###############################################
# CONFIG
###############################################
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP")
RETRY_TOPIC = os.getenv("TOPIC_RETRY", "retry-data")
ERROR_TOPIC = os.getenv("TOPIC_ERROR", "error-data")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

ALERT_INTERVAL_MINUTES = float(os.getenv("ALERT_INTERVAL_MINUTES", 30))
HIGH_TEMPERATURE_THRESHOLD = float(os.getenv("HIGH_TEMPERATURE_THRESHOLD", 31))
LOW_TEMPERATURE_THRESHOLD = float(os.getenv("LOW_TEMPERATURE_THRESHOLD", -10))
RAINFALL_THRESHOLD = float(os.getenv("RAINFALL_THRESHOLD", 11))
WIND_SPEED_THRESHOLD = float(os.getenv("WIND_SPEED_THRESHOLD", 35))
RANDOM_LIMIT = float(os.getenv("RANDOM_LIMIT", 0.01))

# alert 쿨다운 상태
last_alert_time = {}


###############################################
# Helper: Cooldown
###############################################
def should_alert(location, alert_type, event_time):
    key = (location, alert_type)

    if key in last_alert_time:
        diff = (event_time - last_alert_time[key]).total_seconds()
        if diff < ALERT_INTERVAL_MINUTES * 60:
            return False

    last_alert_time[key] = event_time
    return True


###############################################
# Helper: Slack
###############################################
def send_slack(payload: str) -> bool:
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": payload})
        return res.status_code == 200
    except:
        return False


###############################################
# Helper: Alert Type Detection
###############################################
def detect_alert_types(row):
    alerts = []
    try:
        t = float(row.get("Temperature_C", -999))
        p = float(row.get("Precipitation_mm", 0))
        w = float(row.get("Wind_Speed_kmh", 0))
    except:
        return []

    if t >= HIGH_TEMPERATURE_THRESHOLD:
        alerts.append((
            "TEMP_HIGH",
            f"Temperature {t}°C >= {HIGH_TEMPERATURE_THRESHOLD}°C",
            t,
            HIGH_TEMPERATURE_THRESHOLD
        ))

    if t <= LOW_TEMPERATURE_THRESHOLD:
        alerts.append((
            "TEMP_LOW",
            f"Temperature {t}°C <= {LOW_TEMPERATURE_THRESHOLD}°C",
            t,
            LOW_TEMPERATURE_THRESHOLD
        ))

    if p >= RAINFALL_THRESHOLD:
        alerts.append((
            "RAIN_HEAVY",
            f"Rainfall {p}mm >= {RAINFALL_THRESHOLD}mm",
            p,
            RAINFALL_THRESHOLD
        ))

    if w >= WIND_SPEED_THRESHOLD:
        alerts.append((
            "WIND_STRONG",
            f"Wind {w} km/h >= {WIND_SPEED_THRESHOLD} km/h",
            w,
            WIND_SPEED_THRESHOLD
        ))

    return alerts


###############################################
# Kafka Factories
###############################################
def create_consumer():
    return KafkaConsumer(
        RETRY_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id="retry-consumer-group",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="latest"
    )


def create_error_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


###############################################
# Core Logic for each message
###############################################
def handle_message(data, retry_count, error_producer):
    location = data.get("Location", "unknown")

    print(f"\n▶ Processing Retry ({retry_count}): {location}")

    # 🔥 Chaos (랜덤 실패 시뮬레이션)
    if random.random() < RANDOM_LIMIT:
        raise Exception("Intentional Chaos Error (Simulated Failure)")

    # 날짜 파싱
    event_time = datetime.fromisoformat(data["event_time"])

    # 이상 탐지
    triggered = detect_alert_types(data)
    if not triggered:
        print("   ↳ No alert condition met.")
        return

    # 각각의 alert type 별 처리
    for alert_type, reason, value, threshold in triggered:
        if not should_alert(location, alert_type, event_time):
            print(f"   ↳ Cooldown Skipped: {alert_type}")
            continue

        alert_id = save_alert(
            location=location,
            alert_type=alert_type,
            alert_reason=reason,
            event_time=event_time,
            value=value,
            threshold=threshold,
            raw_row=data,
            slack_sent=False,
            retry_count=retry_count
        )

        payload = (
            f"♻️ *Retry Alert ({retry_count})*\n"
            f"Location: {location}\n"
            f"{reason}\n"
            f"Time: {event_time}"
        )

        if send_slack(payload):
            update_alert_sent(alert_id)
            print(f"   ✅ Retry Success: {alert_type} sent.")
        else:
            print(f"   ⚠️ Slack Failed (DB updated)")


###############################################
# Error forwarding
###############################################
def forward_to_error_topic(location, data, retry_count, err, error_producer):
    print(f"❌ Processing Failed: {err}")

    error_payload = {
        "error_type": "CONSUMER3_RETRY_FAIL",
        "error_reason": str(err),
        "raw_row": data,
        "file_name": "processed_by_consumer3",
        "retry_count": retry_count
    }

    error_producer.send(ERROR_TOPIC, key=location, value=error_payload)

    print(f"   ➡️ Forwarded to {ERROR_TOPIC} (retry_count={retry_count})")


###############################################
# Main Loop
###############################################
def run_consumer():
    print(f"🚀 Retry Consumer Started! Topic='{RETRY_TOPIC}' (Chaos={RANDOM_LIMIT*100}%)")

    consumer = create_consumer()
    error_producer = create_error_producer()

    while True:
        try:
            polled = consumer.poll(timeout_ms=1000)

            for tp, messages in polled.items():
                for msg in messages:
                    data = msg.value
                    location = data.get("Location", "unknown")
                    retry_count = int(data.get("retry", 0)) + 1

                    try:
                        handle_message(data, retry_count, error_producer)
                    except Exception as logic_error:
                        forward_to_error_topic(location, data, retry_count, logic_error, error_producer)

        except Exception as e:
            print(f"❌ Critical Consumer Error: {e}")
            time.sleep(3)


###############################################
# Entry Point
###############################################
if __name__ == "__main__":
    run_consumer()
