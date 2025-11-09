# kafka/Dockerfile
FROM apache/kafka:3.7.0

# 초기 토픽 생성 스크립트 복사
COPY create-topics.sh /opt/kafka/create-topics.sh
RUN chmod +x /opt/kafka/create-topics.sh

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/server.properties"]
