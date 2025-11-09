import os, csv, json, time
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

WATCH_DIR = "/app/data/origin-data"
TOPIC_NAME = "weather-data"
BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]

def read_csv_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

class NewFileHandler(FileSystemEventHandler):
    def __init__(self, producer):
        self.producer = producer

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        # time.sleep(0.5)  # 파일 완전히 써질 때까지 잠시 대기
        try:
            rows = read_csv_file(event.src_path)
            for row in rows:
                row["retry"] = 0
                self.producer.send(TOPIC_NAME, value=row)
            self.producer.flush()
            print(f"✅ Sent {len(rows)} rows from {os.path.basename(event.src_path)}")
        except Exception as e:
            print(f"❌ Error processing {event.src_path}: {e}")

def main():
    os.makedirs(WATCH_DIR, exist_ok=True)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    event_handler = NewFileHandler(producer)
    observer = PollingObserver(timeout=1.0)  # ✅ inotify 대신 polling
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
