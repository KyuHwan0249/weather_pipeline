#!/bin/bash
set -e

echo "[INIT] Setup started"

# 1) shared-checkpoints 권한 설정
echo "[INIT] Fixing permissions for /shared-checkpoints..."
mkdir -p /shared-checkpoints
chown -R sparkuser:spark /shared-checkpoints

# 2) Spark 설정 파일 생성 준비 (필수 아님)
echo "[INIT] Running Spark entrypoint setup..."

# Bitnami entrypoint.sh의 1단계(설정 로직)만 실행
/opt/bitnami/scripts/spark/entrypoint.sh echo "Setup done"

# 3) sparkuser로 Spark 실행
echo "[INIT] Starting Spark as sparkuser..."
exec su sparkuser -c "$*"
