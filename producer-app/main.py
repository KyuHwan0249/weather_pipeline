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
processed_files = set()

# 필수 컬럼 정의
REQUIRED_FIELDS = [
    "Location", "Date_Time",
    "Temperature_C", "Humidity_pct",
    "Precipitation_mm", "Wind_Speed_kmh"
]

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
                new_row[f] = 0

    return new_row, None


# ============================================================
# 스키마 검증
# ============================================================
def validate_row(row):
    for f in REQUIRED_FIELDS:
        if f not in row or row[f] == "":
            return False, "MISSING_FIELD", f"Missing required field: {f}"

    converted, err = convert_numeric_fields(row)
    if err:
        return False, "TYPE_ERROR", err

    return True, "OK", converted


# ============================================================
# JSON 로드 & 저장 (지역→파티션)
# ============================================================
def load_region_map():
    global region_partition_map

    if not os.path.exists(MAP_FILE_PATH):
        region_partition_map = {}
        with open(MAP_FILE_PATH, "w") as f:
            json.dump(region_partition_map, f)
        return

    try:
        with open(MAP_FILE_PATH, "r") as f:
            region_partition_map = json.load(f)
    except:
        region_partition_map = {}


def save_region_map():
    with open(MAP_FILE_PATH, "w") as f:
        json.dump(region_partition_map, f, indent=2)


def get_partition_for_region(region):
    if region not in region_partition_map:
        region_partition_map[region] = len(region_partition_map) % NUM_PARTITIONS
        save_region_map()
    return region_partition_map[region]


# ============================================================
# 체크포인트 로드 & 저장
# ============================================================
def load_checkpoint():
    global processed_files
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            processed_files = set(data.get("processed_files", []))
        except:
            processed_files = set()
    else:
        processed_files = set()


def save_checkpoint():
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"processed_files": list(processed_files)}, f, indent=2)


def mark_file_processed(path):
    filename = os.path.basename(path)
    processed_files.add(filename)
    save_checkpoint()


def is_file_processed(path):
    filename = os.path.basename(path)
    return filename in processed_files


# ============================================================
# 파일 처리
# ============================================================
def read_csv_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def process_file(path, producer):
    filename = os.path.basename(path)

    if is_file_processed(path):
        print(f"⏭ Skip already processed: {filename}")
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

                producer.send(ERROR_TOPIC, key=location.encode(), value=error_data)
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
        print(f"✅ Sent {success_count} rows  (errors={error_count})")

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")


# ============================================================
# 디렉토리 감시
# ============================================================
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, producer):
        self.producer = producer

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
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
            print(f"[WAIT] Kafka not ready... retry {i+1}/20")
            time.sleep(3)

    raise Exception("Kafka not available")


# ============================================================
# 메인
# ============================================================
def main():
    os.makedirs(WATCH_DIR, exist_ok=True)

    load_region_map()
    load_checkpoint()

    producer = connect_kafka()

    existing_files = sorted(
        f for f in os.listdir(WATCH_DIR)
        if f.endswith(".csv")
    )

    print(f"🔎 Found {len(existing_files)} existing CSV files")

    for fname in existing_files:
        full_path = os.path.join(WATCH_DIR, fname)
        process_file(full_path, producer)

    event_handler = NewFileHandler(producer)
    observer = PollingObserver(timeout=1.0)
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()

    print(f"👀 Watching directory: {WATCH_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
