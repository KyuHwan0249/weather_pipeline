from datetime import datetime, timedelta
import os
import json
import time
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==========================================
# 환경 변수 및 설정
# ==========================================
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PG_CONN_ID = "postgres_default"

# 1. 실행 주기: 매시 정각
SCHEDULE_INTERVAL = "0 * * * *" 

# 2. 태스크 유지 시간: 3600초 (1시간)
# 1시간 동안 살아있으면서 계속 감시합니다.
RUN_DURATION_SECONDS = 60 * 60 

# 3. 폴링 주기: 5초
POLL_INTERVAL = 5


def monitor_error_logic(**context):
    """
    지정된 시간(1시간) 동안 Loop를 돌며 DB를 감시하고 알람을 보냅니다.
    """
    # 시작 시간과 종료 예정 시간 계산
    start_time = time.time()
    end_time = start_time + RUN_DURATION_SECONDS
    
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    print(f"🚀 Start Monitoring Loop for {RUN_DURATION_SECONDS} seconds. (Check every {POLL_INTERVAL}s)")

    # 1시간이 될 때까지 무한 반복
    while time.time() < end_time:
        try:
            # -------------------------------------------------------
            # 1. DB 조회: 아직 Slack을 안 보낸(slack_sent = false) 데이터
            # -------------------------------------------------------
            # 한 번에 최대 10개씩 가져와서 처리 (에러 폭주 대비)
            rows = hook.get_records("""
                SELECT id, error_type, error_message, location, file_name, raw_row, created_at
                FROM error_weather_data
                WHERE slack_sent = false
                ORDER BY created_at ASC
                LIMIT 10; 
            """)

            # 데이터가 없으면 잠시 대기 후 다시 체크
            if not rows:
                time.sleep(POLL_INTERVAL)
                continue

            # -------------------------------------------------------
            # 2. 데이터 순회 및 처리
            # -------------------------------------------------------
            for row in rows:
                (id, error_type, error_message, location, file_name, raw_row, created_at) = row
                
                print(f"🚨 Error Found: {error_type} (ID: {id}) at {location}")

                # JSON 데이터 안전하게 문자열로 변환
                try:
                    if isinstance(raw_row, dict):
                        raw_str = json.dumps(raw_row, indent=2, ensure_ascii=False)
                    else:
                        raw_str = str(raw_row)
                except:
                    raw_str = "Could not serialize raw data"

                # Slack 메시지 구성
                message = (
                    f"🚨 *Error Detected*\n"
                    f"• Type: `{error_type}`\n"
                    f"• Location: `{location}`\n"
                    f"• File: `{file_name}`\n"
                    f"• Reason: {error_message}\n"
                    f"• Raw Row: ```{raw_str}```\n"
                    f"• Time: {created_at}"
                )

                # Slack 전송 시도
                sent_success = False
                try:
                    res = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
                    if res.status_code == 200:
                        sent_success = True
                    else:
                        print(f"❌ Slack send failed (ID: {id}): {res.text}")
                except Exception as slack_err:
                    print(f"❌ Slack connection error (ID: {id}): {slack_err}")

                # -------------------------------------------------------
                # 3. 전송 성공 시에만 DB 업데이트
                # -------------------------------------------------------
                if sent_success:
                    hook.run(f"""
                        UPDATE error_weather_data
                        SET slack_sent = true,
                            slack_sent_at = NOW()
                        WHERE id = {id};
                    """)
                    print(f"✅ Alert Sent & DB Updated (ID: {id})")
            
            # 배치 처리 후 CPU 과부하 방지를 위해 아주 잠깐 대기
            time.sleep(1)

        except Exception as e:
            # 루프 도중 예상치 못한 에러가 나도 태스크가 죽지 않도록 방어
            print(f"⚠️ Unexpected Error in loop: {e}")
            time.sleep(POLL_INTERVAL)

    print("👋 1 Hour passed. Finishing task successfully. Next run will start immediately.")


# ==========================================
# DAG 정의
# ==========================================
with DAG(
    dag_id="error_alert_hourly_continuous",  # DAG ID
    start_date=datetime(2025, 1, 1),
    schedule_interval=SCHEDULE_INTERVAL,     # "0 * * * *" (1시간마다 실행)
    catchup=False,                           # 과거 데이터 실행 안 함
    max_active_runs=1,                       # [중요] 동시에 1개만 실행 (겹침 방지)
    tags=["monitoring", "weather", "kafka"]
) as dag:

    monitor_task = PythonOperator(
        task_id="monitor_errors_loop",
        python_callable=monitor_error_logic,
        # [중요] 타임아웃은 실행 시간(60분)보다 넉넉하게 70분 설정
        # 60분이 지나면 함수가 스스로 종료되므로 이 타임아웃에 걸릴 일은 거의 없음 (안전장치)
        execution_timeout=timedelta(minutes=70) 
    )
