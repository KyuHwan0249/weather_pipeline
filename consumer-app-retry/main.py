import os
import json
import time
import requests
import random  # 🎲 랜덤 모듈 추가
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# 알람 저장용 DB 모듈 (기존과 동일)
from db.alert_repository import save_alert, update_alert_sent

###############################################
# CONFIG & CONSTANTS
###############################################
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
RETRY_TOPIC = "retry-data"      # 읽어올 토픽
ERROR_TOPIC = "error-data"      # 실패 시 보낼 토픽

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ALERT_INTERVAL_MINUTES = 30

# 쿨다운 상태 관리
last_alert_time = {}

###############################################
# Helper Functions
###############################################
def should_alert(location, alert_type, event_time):
    """쿨다운 체크 로직"""
    key = (location, alert_type)
    if key in last_alert_time:
        diff = (event_time - last_alert_time[key]).total_seconds()
        if diff < ALERT_INTERVAL_MINUTES * 60:
            return False
    last_alert_time[key] = event_time
    return True

def send_slack(payload):
    """Slack 전송"""
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": payload})
        return res.status_code == 200
    except:
        return False

def detect_alert_types(row):
    """임계치 초과 여부 감지"""
    alerts = []
    try:
        t = float(row.get("Temperature_C", -999))
        p = float(row.get("Precipitation_mm", 0))
        w = float(row.get("Wind_Speed_kmh", 0))
    except:
        return []

    if t >= 31: alerts.append(("TEMP_HIGH", f"Temperature {t}°C >= 31°C", t, 31.0))
    if t <= -10: alerts.append(("TEMP_LOW", f"Temperature {t}°C <= -10°C", t, -10.0))
    if p >= 11: alerts.append(("RAIN_HEAVY", f"Rainfall {p}mm >= 11mm", p, 11.0))
    if w >= 35: alerts.append(("WIND_STRONG", f"Wind {w} km/h >= 35 km/h", w, 35.0))
    
    return alerts

###############################################
# Kafka Setup
###############################################
consumer = KafkaConsumer(
    RETRY_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="retry-consumer-group",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="earliest"
)

error_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"🚀 Consumer3 Started with 50% CHAOS MODE. Polling '{RETRY_TOPIC}'...")

###############################################
# MAIN LOOP
###############################################
while True:
    try:
        polled = consumer.poll(timeout_ms=1000)

        for tp, messages in polled.items():
            for msg in messages:
                data = msg.value
                location = data.get("Location", "unknown")
                
                # 재시도 횟수 초기화
                retry_count = int(data.get("retry", 0)) + 1 

                try:
                    print(f"\n▶ Processing Retry ({retry_count}): {location}")

                    # ====================================================
                    # 🎲 [CHAOS ZONE] 50% 확률로 강제 에러 발생
                    # ====================================================
                    if random.random() < 0.5:
                        print(f"   💣 [CHAOS] Simulating Intentional Failure for {location}...")
                        raise Exception("Intentional Chaos Error (Simulated 50% Failure)")
                    # ====================================================
                    
                    # 날짜 파싱
                    event_time = datetime.fromisoformat(data["event_time"])
                    
                    # 이상 징후 감지
                    triggered = detect_alert_types(data)
                    
                    if not triggered:
                        print("   ↳ No alert condition met.")
                        continue

                    for alert_type, reason, value, threshold in triggered:
                        if not should_alert(location, alert_type, event_time):
                            print(f"   ↳ Cooldown skipping: {alert_type}")
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
                            print(f"   ⚠️ Retry DB Saved, Slack Failed.")

                except Exception as logic_error:
                    # ==========================================
                    # [에러 발생 시 -> Error Topic 전송]
                    # ==========================================
                    print(f"❌ Processing Failed: {logic_error}")
                    
                    error_payload = {
                        "error_type": "CONSUMER3_RETRY_FAIL",
                        "error_reason": str(logic_error),  # "Intentional Chaos Error..." 가 담김
                        "raw_row": data,
                        "file_name": "processed_by_consumer3",
                        "retry_count": retry_count
                    }

                    error_producer.send(
                        ERROR_TOPIC,
                        key=location,
                        value=error_payload
                    )
                    
                    error_producer.flush() 
                    print(f"   ➡️ Forwarded to {ERROR_TOPIC} with retry_count={retry_count}")

    except Exception as e:
        print(f"❌ Critical Consumer Error: {e}")
        time.sleep(3)