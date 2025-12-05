#!/bin/bash
set -e

echo "🛑 Stopping all containers..."
docker compose down -v

echo ""
echo "🧱 Generating cluster ID..."
CLUSTER_ID=$(uuidgen)
echo "📌 CLUSTER_ID = $CLUSTER_ID"

############################################################
# FUNCTION: SAFE CLEAR LOCAL DIRECTORIES
############################################################
safe_clear() {
  TARGET=$1
  echo "🗑 Clearing $TARGET ..."
  docker run --rm -v $TARGET:/data busybox sh -c \
    "find /data -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} +"
}

############################################################
# 1) REMOVE OLD DOCKER VOLUMES
############################################################
echo ""
echo "🗑 Removing old Kafka volumes..."
docker volume rm -f weather_pipeline_kafka-1-data || true
docker volume rm -f weather_pipeline_kafka-2-data || true
docker volume rm -f weather_pipeline_kafka-3-data || true

echo ""
echo "🗑 Removing old Spark & Grafana volumes..."
docker volume rm -f weather_pipeline_spark-checkpoints || true
docker volume rm -f weather_pipeline_grafana-data || true

############################################################
# 2) CREATE NEW VOLUMES
############################################################
echo ""
echo "📦 Creating fresh Kafka volumes..."
docker volume create weather_pipeline_kafka-1-data
docker volume create weather_pipeline_kafka-2-data
docker volume create weather_pipeline_kafka-3-data

echo "📦 Creating Spark checkpoint volume..."
docker volume create weather_pipeline_spark-checkpoints

echo "📦 Creating Grafana data volume..."
docker volume create weather_pipeline_grafana-data

############################################################
# 3) FORMAT KAFKA STORAGE
############################################################
echo ""
echo "⚙ Formatting Kafka storage..."

format_kafka() {
  BROKER_NAME=$1
  PROP_FILE=$2
  VOLUME_NAME=$3

  echo "📌 Formatting $BROKER_NAME ..."

  docker run --rm \
    -v $VOLUME_NAME:/var/lib/kafka/data \
    -v $(pwd)/kafka/$PROP_FILE:/opt/kafka/config/kraft/server.properties \
    apache/kafka:3.7.0 \
    bash -c "/opt/kafka/bin/kafka-storage.sh format \
      -t $CLUSTER_ID \
      -c /opt/kafka/config/kraft/server.properties \
      --ignore-formatted"
}

format_kafka "broker1" "server-1.properties" "weather_pipeline_kafka-1-data"
format_kafka "broker2" "server-2.properties" "weather_pipeline_kafka-2-data"
format_kafka "broker3" "server-3.properties" "weather_pipeline_kafka-3-data"


############################################################
# 4) CLEAR LOCAL PROJECT FOLDERS
############################################################

echo ""
echo "🗑 Clearing MinIO data..."
safe_clear "$(pwd)/minio-data"

echo "🗑 Clearing Postgres data..."
safe_clear "$(pwd)/pgdata"

echo "🗑 Clearing Spark checkpoints..."
safe_clear "$(pwd)/spark-checkpoints"

echo "🗑 Clearing Origin data..."
safe_clear "$(pwd)/origin-data"

echo "🗑 Clearing Airflow logs..."
safe_clear "$(pwd)/airflow/logs"

echo "🗑 Clearing Producer checkpoints..."
rm -f producer-app/producer_checkpoint.json || true
rm -f producer-app/region_partition_map.json || true

echo "🗑 Clearing Origin Generator state..."
safe_clear "$(pwd)/create-origin-data-app/state"

echo "🗑 Clearing Origin output files..."
rm -f origin-data/* || true

############################################################
# FINISH
############################################################

echo ""
echo "🎉 Reset complete!"
echo "▶ Run: docker compose up -d"
echo "🧩 Kafka cluster formatted with CLUSTER_ID = $CLUSTER_ID"
