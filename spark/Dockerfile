FROM bitnamilegacy/spark:3.5.6

USER root

# sparkuser 생성
RUN groupadd -g 1001 spark && \
    useradd -m -u 1001 -g 1001 sparkuser

# init.sh 추가
COPY init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

# 🔥 진짜 entrypoint로 교체 (bitnami entrypoint 완전 무시)
ENTRYPOINT ["/usr/local/bin/init.sh"]

# CMD에는 bitnami run.sh만 전달
CMD ["/opt/bitnami/scripts/spark/run.sh"]
