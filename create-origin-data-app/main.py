import os
import time
import json
import uuid
import random
import argparse
import pandas as pd
from datetime import datetime, timedelta


# ---------------------------------------------------------
# 🔥 랜덤 에러 삽입 함수
# ---------------------------------------------------------
def inject_random_errors(df, error_rate=0.05):
    df = df.copy()

    numeric_fields = ["Temperature_C", "Humidity_pct", "Precipitation_mm", "Wind_Speed_kmh"]
    required_fields = ["Location", "Date_Time"] + numeric_fields

    if error_rate < 0 or error_rate > 1:
        error_rate = 0.05

    for idx in df.index:
        if random.random() > error_rate:
            continue

        error_type = random.choice(["missing_field", "numeric_corrupt", "date_corrupt"])

        if error_type == "missing_field":
            field = random.choice(required_fields)
            df.loc[idx, field] = None
            print(f"[ERROR_INJECT] Missing field → row {idx} '{field}' removed")

        elif error_type == "numeric_corrupt":
            field = random.choice(numeric_fields)
            df.loc[idx, field] = "abc_xyz"
            print(f"[ERROR_INJECT] Corrupted numeric → row {idx} '{field}'='abc_xyz'")

        elif error_type == "date_corrupt":
            df.loc[idx, "Date_Time"] = "2024-99-99 99:99:99"
            print(f"[ERROR_INJECT] Corrupted date → row {idx} invalid")

    return df


# ---------------------------------------------------------
# 🔧 체크포인트 파일 처리
# ---------------------------------------------------------
CHECKPOINT_FILE = "/app/state/origin_generator_checkpoint.json"


def load_last_timestamp(start_time):
    """체크포인트 없으면 start_time부터 시작"""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)

    if not os.path.exists(CHECKPOINT_FILE):
        print("[CHECKPOINT] No checkpoint found. Starting fresh.")
        return start_time

    try:
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
        ts = datetime.strptime(data["last_timestamp"], "%Y-%m-%d %H:%M:%S")
        print(f"[CHECKPOINT] Resuming from {ts}")
        return ts
    except Exception as e:
        print(f"[CHECKPOINT] Failed to load checkpoint: {e}")
        return start_time


def save_last_timestamp(ts):
    """현재 생성이 완료된 시점 기록"""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"last_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
        print(f"[CHECKPOINT] Saved last timestamp = {ts}")
    except Exception as e:
        print(f"[CHECKPOINT] Failed to save checkpoint: {e}")


# ---------------------------------------------------------
# 기존 함수들 유지
# ---------------------------------------------------------
def clean_output_dir(output_path: str):
    """output 디렉토리를 정리 ('.gitkeep' 제외)"""
    os.makedirs(output_path, exist_ok=True)

    removed = 0
    for file in os.listdir(output_path):
        fp = os.path.join(output_path, file)
        if os.path.isfile(fp) and not file.endswith(".gitkeep"):
            os.remove(fp)
            removed += 1
    print(f"[CLEANUP] {removed}개의 기존 파일 삭제 완료")


def generate_origin_files(
    input_path: str,
    output_path: str,
    drop_interval_sec: int,
    row_interval_sec: int,
    error_rate: float = 0.05
):
    df = pd.read_csv(input_path)
    df['Date_Time'] = pd.to_datetime(df['Date_Time'])
    df = df.sort_values('Date_Time').reset_index(drop=True)

    start_time = df['Date_Time'].min().replace(second=0, microsecond=0)
    end_time = df['Date_Time'].max().replace(second=0, microsecond=0)

    print(f"[INFO] 총 {len(df)}행")
    print(f"[INFO] 시뮬레이션 기간: {start_time} ~ {end_time}")
    print(f"[INFO] {drop_interval_sec}초마다 파일 생성")
    print(f"[INFO] {row_interval_sec}초 단위로 row 분리")

    # ▶ resume 시간 불러오기
    current_time = load_last_timestamp(start_time)

    while current_time <= end_time:
        next_time = current_time + timedelta(seconds=row_interval_sec)
        chunk = df[(df['Date_Time'] >= current_time) & (df['Date_Time'] < next_time)]

        if not chunk.empty:
            chunk = inject_random_errors(chunk, error_rate=error_rate)

            temp_name = os.path.join(output_path, str(uuid.uuid4()))
            ts_str = current_time.strftime("%Y%m%d_%H%M%S")
            final_name = os.path.join(output_path, f"weather_{ts_str}.csv")

            chunk.to_csv(temp_name, index=False)
            os.rename(temp_name, final_name)

            print(f"[WRITE] {final_name} ({len(chunk)} rows)")
        else:
            print(f"[SKIP] {current_time} ~ {next_time} 데이터 없음")

        # ▶ 현재 시점까지 생성 완료 저장
        save_last_timestamp(current_time)

        current_time = next_time
        time.sleep(drop_interval_sec)

    print("[DONE] 모든 origin 데이터 생성 완료!")


def main():
    parser = argparse.ArgumentParser(description="Weather data simulator")

    parser.add_argument("--input", type=str,
                        default=os.getenv("ORIGIN_INPUT", "/app/data/kaggle/weather_data.csv"))
    parser.add_argument("--output", type=str,
                        default=os.getenv("ORIGIN_OUTPUT", "/app/data/output"))
    parser.add_argument("--drop_interval", type=int,
                        default=int(os.getenv("DROP_INTERVAL", "5")))
    parser.add_argument("--row_interval", type=int,
                        default=int(os.getenv("ROW_INTERVAL", "60")))
    parser.add_argument("--error_rate", type=float,
                        default=float(os.getenv("ERROR_RATE", "0.05")))

    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    # ❗ reset되지 않는 이상 기존 파일을 삭제하지 않도록 변경할 수 있음
    # clean_output_dir(args.output)

    generate_origin_files(
        input_path=args.input,
        output_path=args.output,
        drop_interval_sec=args.drop_interval,
        row_interval_sec=args.row_interval,
        error_rate=args.error_rate
    )


if __name__ == "__main__":
    main()
