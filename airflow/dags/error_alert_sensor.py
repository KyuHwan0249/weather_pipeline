from datetime import datetime, timedelta
import os
import json
import time
import requests

from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PG_CONN_ID = "postgres_default"


# -----------------------------
# 커스텀 Continuous Sensor
# -----------------------------
class ContinuousErrorSensor(BaseSensorOperator):

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        # NEW 상태 row 하나 조회
        row = hook.get_first("""
            SELECT id, error_type, error_message, location, file_name, raw_row, created_at
            FROM error_weather_data
            WHERE slack_sent = 'N'
            ORDER BY created_at ASC
            LIMIT 1;
        """)

        if not row:
            # 에러 없음 → 5초 뒤 다시 호출됨
            return False

        (id, error_type, error_message, location, file_name, raw_row, created_at) = row

        # Slack 메시지
        message = (
            f"🚨 *Error Detected*\n"
            f"• Type: `{error_type}`\n"
            f"• Location: `{location}`\n"
            f"• File: `{file_name}`\n"
            f"• Reason: {error_message}\n"
            f"• Raw Row: ```{json.dumps(raw_row, indent=2)}```\n"
            f"• Time: {created_at}"
        )

        res = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        if res.status_code != 200:
            raise Exception(f"Slack send failed: {res.text}")

        # Slack 전송 성공 → 상태 업데이트
        hook.run(f"""
            UPDATE error_weather_data
            SET slack_sent = 'Y',
                slack_sent_at = NOW()
            WHERE id = {id};
        """)

        # Slack 보냈지만 sensor를 끝내지 않고 계속 돌아야 하므로
        # False 반환해서 다시 waiting 상태로 들어감
        return False



# -----------------------------
# DAG 정의
# -----------------------------

with DAG(
    dag_id="error_alert_continuous",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@once",         # 딱 한 번만 실행
    catchup=False,
) as dag:

    monitor_errors = ContinuousErrorSensor(
        task_id="monitor_errors",
        poke_interval=5,                # 5초마다 체크
        timeout=60 * 60 * 24 * 365 * 100,                   # 무한 실행
        mode="reschedule",              # Worker 점유 안함
    )
