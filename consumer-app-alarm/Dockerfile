FROM python:3.12-slim

# root
USER root

# 유저 생성
RUN groupadd -g 1001 spark && \
    useradd -m -u 1001 -g 1001 sparkuser

# Java 설치
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# PySpark & 의존성 설치
RUN pip install pyspark==3.5.6 psycopg2-binary requests slack-sdk

# Kafka jar 폴더
RUN mkdir -p /app/jars

# 환경변수
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY . .

RUN chown -R sparkuser:spark /app

# sparkuser로 실행
USER sparkuser
ENV USER=sparkuser
ENV HOME=/home/sparkuser

# checkpoint 삭제 후 실행
CMD ["bash", "-c", "rm -rf /shared-checkpoints/* && python /app/main.py"]

