#!/bin/bash
set -e

echo "🛑 Stopping all containers..."
docker compose down -v


echo ""
echo "🧱 Generating cluster ID for Kafka KRaft..."
CLUSTER_ID=$(uuidgen)
echo "📌 Using cluster ID: $CLUSTER_ID"


# Function to safely clear folder contents except .gitkeep
safe_clear() {
  TARGET=$1
  echo "🗑 Clearing $TARGET ..."
  docker run --rm -v $TARGET:/data busybox sh -c \
    "find /data -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} +"
}

echo ""
echo "🧱 Reformatting Kafka storage with unified cluster ID..."

# FORMAT FUNCTION
format_kafka() {
  BROKER=$1
  PROP=$2

  echo "⚙ Formatting $BROKER ..."
  docker run --rm \
    -v $(pwd)/kafka/data/$BROKER:/var/lib/kafka/data \
    -v $(pwd)/kafka/$PROP:/opt/kafka/config/kraft/server.properties \
    apache/kafka:3.7.0 \
    bash -c "/opt/kafka/bin/kafka-storage.sh format \
      -t $CLUSTER_ID \
      -c /opt/kafka/config/kraft/server.properties \
      --ignore-formatted"
}

format_kafka "broker1" "server-1.properties"
format_kafka "broker2" "server-2.properties"
format_kafka "broker3" "server-3.properties"

sudo chown -R 1001:1001 kafka/data/broker1
sudo chown -R 1001:1001 kafka/data/broker2
sudo chown -R 1001:1001 kafka/data/broker3

echo ""
echo "🗑 Clearing MinIO data..."
safe_clear "$(pwd)/minio-data"


echo ""
echo "🗑 Clearing Postgres data..."
safe_clear "$(pwd)/pgdata"


echo ""
echo "🗑 Clearing Spark checkpoints..."
safe_clear "$(pwd)/spark-checkpoints"


echo ""
echo "🗑 Clearing Origin data folder..."
safe_clear "$(pwd)/origin-data"


echo ""
echo "🗑 Clearing Airflow logs..."
safe_clear "$(pwd)/airflow/logs"


echo ""
echo "🗑 Clearing Producer checkpoints..."
rm -f producer-app/producer_checkpoint.json || true
rm -f producer-app/region_partition_map.json || true


echo "🗑 Clearing Origin Generator checkpoint..."
safe_clear "$(pwd)/create-origin-data-app/state"


echo "🗑 Clearing Origin output files..."
rm -f origin-data/* || true


echo ""
echo "🎉 Reset complete!"
echo "▶ Run: docker compose up -d"
echo "   Kafka cluster reformatted with cluster ID: $CLUSTER_ID"
