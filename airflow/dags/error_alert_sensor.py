from datetime import datetime, timedelta
import os
import json
import requests

from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PG_CONN_ID = "postgres_default"


# Sensor에서 사용할 SQL
CHECK_SQL = """
SELECT id FROM error_weather_data
WHERE slack_sent = 'N'
LIMIT 1;
"""


# 1) Slack 보낼 row 가져오고 slack 보내기
def send_slack(**context):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    row = hook.get_first("""
        SELECT id, error_type, error_message, location, file_name, raw_row, created_at
        FROM error_weather_data
        WHERE slack_sent = 'N'
        ORDER BY created_at ASC
        LIMIT 1;
    """)

    if not row:
        print("No rows to process")
        return None

    (id, error_type, error_message, location, file_name, raw_row, created_at) = row

    # Slack message formatting
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

    print("Slack sent OK. Returning id:", id)
    # 이 값을 Update task로 전달
    return id



# ----------------------------------------
# DAG
# ----------------------------------------

with DAG(
    dag_id="error_alert_sensor",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",  # 2분 주기
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    }
) as dag:

    # 새로운 에러가 있는지 감지하는 Sensor
    wait_for_error = SqlSensor(
        task_id="wait_for_error",
        conn_id=PG_CONN_ID,
        sql=CHECK_SQL,
        poke_interval=20,
        timeout=600,
        mode="poke"
    )

    # Slack 보내기
    send_slack_task = PythonOperator(
        task_id="send_slack_task",
        python_callable=send_slack
    )

    # Slack 정상 전송 후 DB 업데이트
    update_status = PostgresOperator(
        task_id="update_status",
        postgres_conn_id=PG_CONN_ID,
        sql="""
        UPDATE error_weather_data
        SET 
            slack_sent = 'Y',
            slack_sent_at = NOW()
        WHERE id = {{ ti.xcom_pull(task_ids='send_slack_task') }};
        """
    )

    wait_for_error >> send_slack_task >> update_status
