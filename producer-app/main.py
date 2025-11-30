import os
import csv
import json
import time
from kafka import KafkaProducer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from kafka.errors import NoBrokersAvailable

# ============================================================
# 환경변수 기반 설정
# ============================================================
WATCH_DIR = os.getenv("WATCH_DIR", "/app/data/origin-data")
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-data")
ERROR_TOPIC = os.getenv("TOPIC_ERROR", "error-data")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")

MAP_FILE_PATH = os.getenv("REGION_MAP_FILE", "/app/region_partition_map.json")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "10"))

# 파일 단위 체크포인트 저장 파일
CHECKPOINT_FILE = os.getenv("PRODUCER_CHECKPOINT_FILE", "/app/producer_checkpoint.json")

region_partition_map = {}
processed_files = set()  # 이미 처리 완료한 파일 이름(basename) 저장

# 필수 컬럼 정의
REQUIRED_FIELDS = [
    "Location", "Date_Time",
    "Temperature_C", "Humidity_pct",
    "Precipitation_mm", "Wind_Speed_kmh"
]

# 숫자로 변환해야 하는 필드 리스트
NUMERIC_FIELDS_FLOAT = [
    "Temperature_C",
    "Humidity_pct",
    "Precipitation_mm",
    "Wind_Speed_kmh"
]
NUMERIC_FIELDS_INT = ["retry"]


# ============================================================
# 숫자 자동 변환
# ============================================================
def convert_numeric_fields(row):
    new_row = dict(row)

    for f in NUMERIC_FIELDS_FLOAT:
        if f in new_row:
            try:
                new_row[f] = float(new_row[f])
            except Exception:
                return None, f"Invalid float field: {f}='{new_row[f]}'"

    for f in NUMERIC_FIELDS_INT:
        if f in new_row:
            try:
                new_row[f] = int(new_row[f])
            except Exception:
                new_row[f] = 0  # fallback

    return new_row, None


# ============================================================
# 스키마 검증
# ============================================================
def validate_row(row):
    # 1) 필수 필드 체크
    for f in REQUIRED_FIELDS:
        if f not in row or row[f] == "":
            return False, "MISSING_FIELD", f"Missing required field: {f}"

    # 2) 숫자 변환
    converted, err = convert_numeric_fields(row)
    if err:
        return False, "TYPE_ERROR", err

    return True, "OK", converted


# ============================================================
# JSON 로드 & 저장 (지역→파티션)
# ============================================================
def load_region_map():
    global region_partition_map
    
    # 파일 없으면 빈 파일 생성
    if not os.path.exists(MAP_FILE_PATH):
        print(f"🆕 No region map found. Creating empty map at {MAP_FILE_PATH}")
        region_partition_map = {}
        
        # 바로 생성 (빈 JSON 구조)
        try:
            with open(MAP_FILE_PATH, "w") as f:
                json.dump(region_partition_map, f, indent=2)
        except Exception as e:
            print(f"⚠️ Could not create empty map file: {e}")
        return

    # 파일이 존재하면 읽기
    try:
        with open(MAP_FILE_PATH, "r") as f:
            region_partition_map = json.load(f)
        print(f"🔁 Loaded region map: {region_partition_map}")
    except Exception as e:
        print(f"⚠️ Failed to load region map. Resetting it. Error: {e}")
        region_partition_map = {}


def save_region_map():
    try:
        with open(MAP_FILE_PATH, "w") as f:
            json.dump(region_partition_map, f, indent=2)
        print(f"💾 Region map saved: {region_partition_map}")
    except Exception as e:
        print(f"⚠️ Failed to save region map: {e}")


# ============================================================
# CSV 읽기
# ============================================================
def read_csv_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ============================================================
# 파티션 결정
# ============================================================
def get_partition_for_region(region):
    if region not in region_partition_map:
        region_partition_map[region] = len(region_partition_map) % NUM_PARTITIONS
        save_region_map()
    return region_partition_map[region]


# ============================================================
# 체크포인트 로드 & 저장 (파일 단위)
# ============================================================
def load_checkpoint():
    global processed_files
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            processed_files = set(data.get("processed_files", []))
            print(f"📂 Loaded checkpoint. processed_files={len(processed_files)}")
        except Exception as e:
            print(f"⚠️ Failed to load checkpoint: {e}")
            processed_files = set()
    else:
        processed_files = set()
        print("🆕 No existing checkpoint file.")


def save_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"processed_files": list(processed_files)}, f, indent=2)
    except Exception as e:
        print(f"⚠️ Failed to save checkpoint: {e}")


def mark_file_processed(path):
    """파일 처리가 끝났을 때 basename 기준으로 기록"""
    filename = os.path.basename(path)
    processed_files.add(filename)
    save_checkpoint()


def is_file_processed(path):
    filename = os.path.basename(path)
    return filename in processed_files


# ============================================================
# 파일 하나 처리 (재사용 가능하도록 함수로 분리)
# ============================================================
def process_file(path, producer):
    """단일 CSV 파일 전체를 읽어서 Kafka로 전송"""
    filename = os.path.basename(path)

    if is_file_processed(path):
        print(f"⏭  Skip already processed file: {filename}")
        return

    print(f"📥 Processing file: {filename}")

    try:
        rows = read_csv_file(path)
        success_count = 0
        error_count = 0

        for row in rows:
            row["retry"] = 0
            location = row.get("Location") or "unknown"

            ok, error_type, result = validate_row(row)

            if not ok:
                error_data = {
                    "error_type": error_type,
                    "error_reason": result,
                    "raw_row": row,
                    "timestamp": time.time(),
                    "file_name": filename,
                    "retry_count": 0
                }

                producer.send(
                    ERROR_TOPIC,
                    key=location.encode(),
                    value=error_data
                )
                error_count += 1
                continue

            valid_row = result
            partition = get_partition_for_region(location)

            producer.send(
                TOPIC_NAME,
                key=location.encode(),
                value=valid_row,
                partition=partition
            )
            success_count += 1

        producer.flush()
        mark_file_processed(path)
        print(f"✅ Sent {success_count} rows from {filename} (errors={error_count})")

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")


# ============================================================
# 파일 생성 이벤트 핸들러
# ============================================================
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, producer):
        self.producer = producer

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        # 새로 생성된 파일 처리
        process_file(event.src_path, self.producer)


# ============================================================
# Kafka 연결
# ============================================================
def connect_kafka():
    for i in range(20):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                key_serializer=lambda k: k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except NoBrokersAvailable:
            print(f"[WARN] Kafka not ready... retry {i+1}/20")
            time.sleep(3)
    raise Exception("Kafka not available after retries")


# ============================================================
# 메인 실행
# ============================================================
def main():
    os.makedirs(WATCH_DIR, exist_ok=True)

    load_region_map()
    load_checkpoint()

    producer = connect_kafka()

    # 1) 시작 시 기존 파일들 먼저 처리
    existing_files = sorted(
        f for f in os.listdir(WATCH_DIR)
        if f.endswith(".csv")
    )

    print(f"🔎 Found {len(existing_files)} existing CSV files at startup.")

    for fname in existing_files:
        full_path = os.path.join(WATCH_DIR, fname)
        process_file(full_path, producer)

    # 2) 이후 새로 생성되는 파일 감시
    event_handler = NewFileHandler(producer)
    observer = PollingObserver(timeout=1.0)
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()

    print(f"👀 Watching directory (polling): {WATCH_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
