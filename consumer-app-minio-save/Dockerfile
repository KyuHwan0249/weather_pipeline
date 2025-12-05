FROM python:3.11-slim

WORKDIR /app
COPY main.py /app/
RUN pip install --default-timeout=200 --no-cache-dir kafka-python boto3 pandas pyarrow

CMD ["python", "main.py"]
