FROM bitnamilegacy/spark:3.5

USER root

# ✅ Spark 실행용 유저 추가
ARG USER_ID=1001
RUN echo "sparkuser:x:${USER_ID}:0:sparkuser:/home:/bin/bash" >> /etc/passwd

# ✅ Spark 유저 설정
USER 1001
ENV USER=sparkuser
ENV HOME=/home

WORKDIR /opt/bitnami/spark
