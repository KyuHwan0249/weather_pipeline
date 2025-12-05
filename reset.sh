#!/bin/bash
set -e

echo "🛑 Stopping all containers..."
# -v 옵션은 docker compose가 관리하는 볼륨(kafka data 등)을 자동으로 삭제합니다.
docker compose down -v

############################################################
# FUNCTION: SAFE CLEAR LOCAL DIRECTORIES
############################################################
safe_clear() {
  TARGET=$1
  if [ -d "$TARGET" ]; then
    echo "🗑 Clearing $TARGET ..."
    # 권한 문제 없이 삭제하기 위해 docker 이용
    docker run --rm -v "$TARGET":/data busybox sh -c \
      "find /data -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} +"
  else
    echo "⚠️  $TARGET directory not found, skipping..."
  fi
}

############################################################
# CLEAR LOCAL PROJECT FOLDERS
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

############################################################
# FINISH
############################################################

echo ""
echo "🎉 Reset complete!"
echo "▶ Run: docker compose up -d"
echo "ℹ️  Kafka will format itself automatically on first startup."