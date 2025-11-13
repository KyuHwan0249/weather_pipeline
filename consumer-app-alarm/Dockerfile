FROM python:3.12-slim

WORKDIR /app

# ✅ Java 21 + ps 설치
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# ✅ Spark와 호환되는 PySpark 설치
RUN pip install pyspark==3.5.6

# ✅ 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

COPY main.py .

CMD ["python", "main.py"]
