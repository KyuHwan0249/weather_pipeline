FROM bitnamilegacy/spark:3.5.6

# root 권한으로 유저 생성
USER root

RUN groupadd -g 1001 spark && \
    useradd -m -u 1001 -g 1001 sparkuser

# 홈 디렉토리 소유권 설정
RUN chown -R sparkuser:spark /home/sparkuser

# 실행 유저 변경
USER sparkuser
ENV USER=sparkuser
ENV HOME=/home/sparkuser

WORKDIR /opt/bitnami/spark
