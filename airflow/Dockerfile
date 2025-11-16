FROM apache/airflow:2.8.0

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --user --no-cache-dir -r /requirements.txt
