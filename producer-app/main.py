import os, csv, json, time
from kafka import KafkaProducer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

WATCH_DIR = "/app/data/origin-data"
TOPIC_NAME = "weather-data"
ERROR_TOPIC = "error-data"

BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]

MAP_FILE_PATH = "/app/region_partition_map.json"
NUM_PARTITIONS = 10

region_partition_map = {}

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
            except:
                return None, f"Invalid float field: {f}='{new_row[f]}'"

    for f in NUMERIC_FIELDS_INT:
        if f in new_row:
            try:
                new_row[f] = int(new_row[f])
            except:
                new_row[f] = 0

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
# JSON 로드 & 저장
# ============================================================
def load_region_map():
    global region_partition_map
    if os.path.exists(MAP_FILE_PATH):
        with open(MAP_FILE_PATH, "r") as f:
            region_partition_map = json.load(f)
        print(f"🔁 Loaded region map: {region_partition_map}")
    else:
        region_partition_map = {}
        print("🆕 No existing region map.")


def save_region_map():
    with open(MAP_FILE_PATH, "w") as f:
        json.dump(region_partition_map, f, indent=2)


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
# 파일 생성 이벤트
# ============================================================
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, producer):
        self.producer = producer

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return

        try:
            rows = read_csv_file(event.src_path)

            for row in rows:
                row["retry"] = 0
                location = row.get("Location") or "unknown"

                # ❌ 검증 실패 → error topic으로 전송
                ok, error_type, result = validate_row(row)

                if not ok:
                    error_data = {
                        "error_type": error_type,
                        "error_reason": result,
                        "raw_row": row,
                        "timestamp": time.time(),
                        "file_name": os.path.basename(event.src_path),
                        "retry_count": 0
                    }

                    self.producer.send(
                        ERROR_TOPIC,
                        key=location.encode(),
                        value=error_data
                    )
                    continue
                    # 정상 topic에는 보내지 않음

                # ✔️ 검증 성공 시 result는 변환된 row
                valid_row = result
                partition = get_partition_for_region(location)

                # 정상 토픽 전송
                self.producer.send(
                    TOPIC_NAME,
                    key=location.encode(),
                    value=valid_row,
                    partition=partition
                )

            self.producer.flush()
            print(f"✅ Sent {len(rows)} rows from {os.path.basename(event.src_path)}")

        except Exception as e:
            print(f"❌ Error processing {event.src_path}: {e}")


from kafka.errors import NoBrokersAvailable

def connect_kafka():
    for i in range(20):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
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

    producer = connect_kafka()

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
