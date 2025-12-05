#!/bin/bash
set -e

echo "🛑 Stopping and Removing ALL containers and volumes..."
# -v : Docker Compose에 정의된 Volume(Kafka data, Spark checkpoint 등)을 모두 삭제합니다.
# --rmi local : 로컬 이미지가 꼬이는 것을 방지하기 위해(선택사항이지만 깔끔함을 위해)
docker compose down -v --remove-orphans

############################################################
# Helper Function: 권한 문제 없이 폴더 비우기
############################################################
safe_clear() {
  TARGET=$1
  if [ -d "$TARGET" ]; then
    echo "🗑 Clearing contents of $TARGET ..."
    # Docker를 이용해 삭제하면 root 권한 파일도 깔끔하게 지워집니다.
    docker run --rm -v "$TARGET":/data busybox sh -c \
      "find /data -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} +"
  else
    echo "⚠️  Directory $TARGET not found. Creating it..."
    mkdir -p "$TARGET"
  fi
}

############################################################
# 1) 로컬에 마운트된 데이터 폴더 초기화
############################################################

echo ""
echo "🧹 Cleaning Local Data Folders..."

# Postgres DB 데이터 삭제 (유저 정보, Airflow 메타데이터 등 모두 초기화)
safe_clear "$(pwd)/pgdata"

# MinIO 파일 저장소 삭제 (버킷, 업로드된 파일 모두 초기화)
safe_clear "$(pwd)/minio-data"

# Airflow 로그 삭제
safe_clear "$(pwd)/airflow/logs"

# Spark 체크포인트 삭제 (스트리밍 상태 초기화)
# (볼륨으로 잡혀있을 수도 있지만, 로컬 폴더로도 매핑된 경우를 대비)
safe_clear "$(pwd)/spark-checkpoints"

############################################################
# 2) 파이프라인 관련 상태 파일 삭제 (가장 중요)
############################################################

echo ""
echo "🧹 Cleaning Pipeline State..."

# [중요] 생성된 원본 데이터 삭제 (다시 처음부터 생성하도록)
safe_clear "$(pwd)/origin-data"

# [중요] Producer 앱의 체크포인트 삭제
# (이게 남아있으면 Producer가 '이미 읽었다'고 판단해서 데이터를 안 보낼 수 있음)
rm -f producer-app/producer_checkpoint.json || true
rm -f producer-app/region_partition_map.json || true
# 만약 파이썬 캐시가 꼬일 수 있으므로 pycache도 삭제
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Origin Data Generator의 상태 삭제
safe_clear "$(pwd)/create-origin-data-app/state"

############################################################
# 3) 완료 메시지
############################################################

echo ""
echo "✨ All data has been wiped! (Hard Reset Complete)"
echo "---------------------------------------------------"
echo "🚀 Now run: docker compose up -d"
echo "   -> Kafka will re-format automatically."
echo "   -> Postgres will re-initdb."
echo "   -> MinIO will start empty."
echo "   -> Data pipeline will start from scratch."