from datetime import datetime, timedelta
import os
import requests
import json

from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PG_CONN_ID = "postgres_default"   # airflow connection 이름


# 1) Sensor가 실행할 SQL
CHECK_SQL = """
SELECT id FROM error_weather_data
WHERE processing_status = 'NEW'
LIMIT 1;
"""


# 2) slack 보내기 + DB 업데이트
def send_slack_and_update(**context):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    rows = hook.get_records("""
        SELECT id, error_type, error_message, location, file_name, raw_row
        FROM error_weather_data
        WHERE processing_status = 'NEW'
        ORDER BY created_at ASC
        LIMIT 1;
    """)

    if not rows:
        print("No NEW error rows found")
        return
    
    row = rows[0]
    (id, error_type, error_message, location, file_name, raw_row) = row

    # Slack 메시지 내용
    message = (
        f"🚨 *Error Detected*\n"
        f"• Type: `{error_type}`\n"
        f"• Location: `{location}`\n"
        f"• File: `{file_name}`\n"
        f"• Reason: {error_message}\n"
        f"• Raw Row: ```{json.dumps(raw_row, indent=2)}```"
    )

    # Slack 전송
    res = requests.post(SLACK_WEBHOOK_URL, json={"text": message})

    if res.status_code == 200:
        print("Slack sent successfully. Updating DB...")
        hook.run("""
            UPDATE error_weather_data
            SET processing_status='SENT'
            WHERE id = %s
        """, parameters=(id,))
    else:
        print("Slack failed", res.text)


# ----------------------------------------
# DAG
# ----------------------------------------

with DAG(
    dag_id="error_alert_sensor",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",  # 2분마다 체크
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10)
    }
) as dag:

    wait_for_error = SqlSensor(
        task_id="wait_for_error",
        conn_id=PG_CONN_ID,
        sql=CHECK_SQL,
        poke_interval=20,     # 20초마다 확인
        timeout=60 * 10,      # 10분 동안 대기
        mode="poke"
    )

    process_error = PythonOperator(
        task_id="process_error",
        python_callable=send_slack_and_update
    )

    wait_for_error >> process_error
