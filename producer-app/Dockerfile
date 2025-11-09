FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y iputils-ping netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

CMD ["python", "main.py"]
