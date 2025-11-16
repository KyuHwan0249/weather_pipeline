FROM apache/airflow:2.8.0

# 필요하면 추가 패키지 설치
USER root

# 예: psycopg2, kafka-python, requests 등
RUN pip install --no-cache-dir \
    psycopg2-binary \
    kafka-python \
    requests

# 다시 airflow 유저로
USER airflow

# Airflow DAG / Plugins 경로 유지
ENV AIRFLOW_HOME=/opt/airflow
