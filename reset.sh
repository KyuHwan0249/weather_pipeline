#!/bin/bash
set -e

echo "🛑 Stopping all containers..."
docker compose down -v


echo ""
echo "🧱 Generating cluster ID for Kafka KRaft..."
CLUSTER_ID=$(uuidgen)
echo "📌 Using cluster ID: $CLUSTER_ID"


echo ""
echo "🗑 Clearing Kafka data..."
docker run --rm -v $(pwd)/kafka/data/broker1:/data busybox sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/kafka/data/broker2:/data busybox sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/kafka/data/broker3:/data busybox sh -c "rm -rf /data/*"

echo ""
echo "🧱 Reformatting Kafka storage with same cluster ID..."

docker run --rm \
  -v $(pwd)/kafka/data/broker1:/var/lib/kafka/data \
  -v $(pwd)/kafka/server-1.properties:/opt/kafka/config/kraft/server.properties \
  apache/kafka:3.7.0 \
  bash -c "/opt/kafka/bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c /opt/kafka/config/kraft/server.properties"


docker run --rm \
  -v $(pwd)/kafka/data/broker2:/var/lib/kafka/data \
  -v $(pwd)/kafka/server-2.properties:/opt/kafka/config/kraft/server.properties \
  apache/kafka:3.7.0 \
  bash -c "/opt/kafka/bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c /opt/kafka/config/kraft/server.properties"


docker run --rm \
  -v $(pwd)/kafka/data/broker3:/var/lib/kafka/data \
  -v $(pwd)/kafka/server-3.properties:/opt/kafka/config/kraft/server.properties \
  apache/kafka:3.7.0 \
  bash -c "/opt/kafka/bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c /opt/kafka/config/kraft/server.properties"


echo ""
echo "🗑 Clearing MinIO data..."
docker run --rm -v $(pwd)/minio-data:/data busybox sh -c "rm -rf /data/*"

echo ""
echo "🗑 Clearing Postgres data..."
docker run --rm -v $(pwd)/pgdata:/var/lib/postgresql/data busybox sh -c "rm -rf /var/lib/postgresql/data/*"

echo ""
echo "🗑 Clearing Spark checkpoints..."
docker run --rm -v $(pwd)/spark-checkpoints:/data busybox sh -c "rm -rf /data/*"

echo ""
echo "🗑 Clearing Origin data..."
docker run --rm -v $(pwd)/origin-data:/data busybox sh -c "rm -rf /data/*"

echo ""
echo "🗑 Clearing Airflow logs..."
docker run --rm -v $(pwd)/airflow/logs:/data busybox sh -c "rm -rf /data/*"

echo ""
echo "🗑 Clearing Producer checkpoint..."
rm -f producer-app/producer_checkpoint.json
echo "🗑 Clearing Region map..."
rm -f producer-app/region_partition_map.json

echo "🗑 Clearing Origin Generator checkpoint..."
docker run --rm -v $(pwd)/create-origin-data-app/state:/data busybox sh -c "rm -f /data/origin_generator_checkpoint.json"

echo "🗑 Clearing Origin output files..."
rm -f origin-data/*

echo ""
echo "🎉 Reset complete!"
echo "▶ Run: docker compose up -d"
echo "   Kafka cluster reformatted with unified cluster ID: $CLUSTER_ID"
echo "   Airflow will auto-init because Postgres is empty."
