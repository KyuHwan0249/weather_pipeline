import os, csv, json, time
from kafka import KafkaProducer
from watchdog.observers.polling import PollingObserver 
from watchdog.events import FileSystemEventHandler

WATCH_DIR = "/app/data/origin-data"
TOPIC_NAME = "weather-data"
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]

# ✅ 지역 파티션 매핑 파일
MAP_FILE_PATH = "/app/region_partition_map.json"
NUM_PARTITIONS = 10

region_partition_map = {}

# 숫자로 변환해야 하는 필드 리스트
NUMERIC_FIELDS_FLOAT = [
    "Temperature_C",
    "Humidity_pct",
    "Precipitation_mm",
    "Wind_Speed_kmh"
]
NUMERIC_FIELDS_INT = ["retry"]

# -----------------------------------------------------------
# 숫자 자동 변환 함수
# -----------------------------------------------------------
def convert_numeric_fields(row):
    new_row = dict(row)

    # Float 타입 처리
    for f in NUMERIC_FIELDS_FLOAT:
        if f in new_row:
            try:
                new_row[f] = float(new_row[f])
            except:
                print(f"⚠️ Failed to convert {f}='{new_row[f]}' → setting None")
                new_row[f] = None

    # Int 타입 처리
    for f in NUMERIC_FIELDS_INT:
        if f in new_row:
            try:
                new_row[f] = int(new_row[f])
            except:
                new_row[f] = 0

    return new_row

# -----------------------------------------------------------
# JSON 로드 & 저장
# -----------------------------------------------------------
def load_region_map():
    global region_partition_map
    if os.path.exists(MAP_FILE_PATH):
        try:
            with open(MAP_FILE_PATH, "r") as f:
                region_partition_map = json.load(f)
            print(f"🔁 Loaded region map: {region_partition_map}")
        except:
            region_partition_map = {}
    else:
        print("🆕 No existing region map.")

def save_region_map():
    try:
        with open(MAP_FILE_PATH, "w") as f:
            json.dump(region_partition_map, f, indent=2)
    except Exception as e:
        print(f"⚠️ Failed to save region map: {e}")

# -----------------------------------------------------------
# CSV 읽기
# -----------------------------------------------------------
def read_csv_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# -----------------------------------------------------------
# 지역 파티션 결정
# -----------------------------------------------------------
def get_partition_for_region(region):
    if region not in region_partition_map:
        region_partition_map[region] = len(region_partition_map) % NUM_PARTITIONS
        print(f"[🆕] '{region}' → partition {region_partition_map[region]}")
        save_region_map()
    return region_partition_map[region]

# -----------------------------------------------------------
# 파일 생성 이벤트 처리
# -----------------------------------------------------------
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

                # 🔥 숫자 필드 변환 실행
                row = convert_numeric_fields(row)

                # 🔥 지역 파티션 결정
                partition = get_partition_for_region(location)

                # Kafka 전송
                self.producer.send(
                    TOPIC_NAME,
                    key=location.encode("utf-8"),
                    value=row,
                    partition=partition
                )

            self.producer.flush()
            print(f"✅ Sent {len(rows)} rows from {os.path.basename(event.src_path)}")

        except Exception as e:
            print(f"❌ Error processing {event.src_path}: {e}")

# -----------------------------------------------------------
# 메인 실행
# -----------------------------------------------------------
def main():
    os.makedirs(WATCH_DIR, exist_ok=True)
    load_region_map()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

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
