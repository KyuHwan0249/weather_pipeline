FROM python:3.12-slim

# root
USER root

# 패키지 업데이트 + psycopg2 의존성 설치
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# 필요한 Python 패키지 설치
RUN pip install --no-cache-dir \
    kafka-python \
    psycopg2-binary \
    requests

# 작업 디렉토리
WORKDIR /app

# 현재 디렉토리의 파일 복사 (여기 반드시 main.py 있어야 함)
COPY . .

# 실행
CMD ["python", "-u", "main.py"]
