import os, csv, json, time
from kafka import KafkaProducer
from watchdog.observers.polling import PollingObserver 
from watchdog.events import FileSystemEventHandler

WATCH_DIR = "/app/data/origin-data"
TOPIC_NAME = "weather-data"
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]

# ✅ 파티션 매핑 저장 파일 경로
MAP_FILE_PATH = "/app/region_partition_map.json"
NUM_PARTITIONS = 10  # Kafka 토픽 파티션 수와 동일하게 설정

# ✅ 지역별 파티션 매핑 딕셔너리
region_partition_map = {}

# -----------------------------------------------------------
# JSON 파일로 매핑 정보 로드/저장
# -----------------------------------------------------------
def load_region_map():
    global region_partition_map
    if os.path.exists(MAP_FILE_PATH):
        try:
            with open(MAP_FILE_PATH, "r") as f:
                region_partition_map = json.load(f)
            print(f"🔁 Loaded existing region map: {region_partition_map}")
        except Exception as e:
            print(f"⚠️ Failed to load region map: {e}")
            region_partition_map = {}
    else:
        print("🆕 No existing region map found, starting fresh.")

def save_region_map():
    try:
        with open(MAP_FILE_PATH, "w") as f:
            json.dump(region_partition_map, f, indent=2)
    except Exception as e:
        print(f"⚠️ Failed to save region map: {e}")

# -----------------------------------------------------------
# CSV 읽기 함수
# -----------------------------------------------------------
def read_csv_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# -----------------------------------------------------------
# 지역별 파티션 번호 부여 로직
# -----------------------------------------------------------
def get_partition_for_region(region):
    if region not in region_partition_map:
        region_partition_map[region] = len(region_partition_map) % NUM_PARTITIONS
        print(f"[🆕] Assigned partition {region_partition_map[region]} for region '{region}'")
        save_region_map()
    return region_partition_map[region]

# -----------------------------------------------------------
# 파일 생성 이벤트 핸들러
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
                location = row.get("Location") or "unknown"
                row["retry"] = 0

                # ✅ 지역별 파티션 결정
                partition = get_partition_for_region(location)

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
# 메인 함수
# -----------------------------------------------------------
def main():
    os.makedirs(WATCH_DIR, exist_ok=True)
    load_region_map()  # ✅ 기존 매핑 불러오기

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    event_handler = NewFileHandler(producer)
    observer = PollingObserver(timeout=1.0)  # ✅ polling 모드
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
