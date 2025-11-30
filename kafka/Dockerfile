FROM apache/kafka:3.7.0

# Nothing copied: server-X.properties will be mounted by docker-compose
ENTRYPOINT ["/bin/bash"]
CMD ["/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/server.properties"]
