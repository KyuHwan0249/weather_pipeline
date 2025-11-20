import os
import time
import pandas as pd
import argparse
import uuid
import random
from datetime import datetime, timedelta


# ---------------------------------------------------------
# 🔥 랜덤 에러 삽입 함수
# ---------------------------------------------------------
def inject_random_errors(df, error_rate=0.05):
    """
    df: chunk dataframe
    error_rate: 0.05 → 5% 확률로 문제있는 row 생성
    """
    df = df.copy()

    numeric_fields = ["Temperature_C", "Humidity_pct", "Precipitation_mm", "Wind_Speed_kmh"]
    required_fields = ["Location", "Date_Time"] + numeric_fields
    error_rate = -1
    for idx in df.index:
        if random.random() > error_rate:
            continue  # 에러 없음

        error_type = random.choice(["missing_field", "numeric_corrupt", "date_corrupt"])

        # 1️⃣ 필수 필드 누락
        if error_type == "missing_field":
            field_to_remove = random.choice(required_fields)
            df.loc[idx, field_to_remove] = None
            print(f"[ERROR_INJECT] Missing field → row {idx} field '{field_to_remove}' removed")

        # 2️⃣ 숫자 필드 깨기
        elif error_type == "numeric_corrupt":
            field = random.choice(numeric_fields)
            df.loc[idx, field] = "abc_xyz"
            print(f"[ERROR_INJECT] Corrupted numeric → row {idx} field '{field}' = 'abc_xyz'")

        # 3️⃣ 날짜 필드 깨기
        elif error_type == "date_corrupt":
            df.loc[idx, "Date_Time"] = "2024-99-99 99:99:99"
            print(f"[ERROR_INJECT] Corrupted date → row {idx} Date_Time invalid format")

    return df


# ---------------------------------------------------------
# 기존 함수들 유지
# ---------------------------------------------------------
def clean_output_dir(output_path: str):
    """output 디렉토리의 .gitkeep을 제외한 파일 모두 삭제"""
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        return

    removed = 0
    for file in os.listdir(output_path):
        file_path = os.path.join(output_path, file)
        if os.path.isfile(file_path) and not file.endswith(".gitkeep"):
            os.remove(file_path)
            removed += 1
    print(f"[CLEANUP] {removed}개의 기존 파일 삭제 완료 ('.gitkeep'은 유지됨)")


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
    end_time = df['Date_Time'].max().replace(second=0, microsecond=0) + timedelta(minutes=1)

    print(f"[INFO] 총 {len(df)}행, 기간: {start_time} ~ {end_time}")
    print(f"[INFO] {drop_interval_sec}초마다 파일 생성, 데이터 간격 {row_interval_sec}초 단위")

    current_time = start_time

    while current_time <= end_time:
        next_time = current_time + timedelta(seconds=row_interval_sec)
        chunk = df[(df['Date_Time'] >= current_time) & (df['Date_Time'] < next_time)]

        if not chunk.empty:
            # 🚨 여기서 에러 삽입
            chunk = inject_random_errors(chunk, error_rate=error_rate)

            # 1️⃣ 임시 이름 (uuid)
            temp_name = os.path.join(output_path, str(uuid.uuid4()))

            # 2️⃣ 최종 이름
            ts_str = current_time.strftime("%Y%m%d_%H%M%S")
            final_name = os.path.join(output_path, f"weather_{ts_str}.csv")

            # 3️⃣ 저장
            chunk.to_csv(temp_name, index=False)
            print(f"[작성중] {temp_name} ({len(chunk)} rows, {current_time}~{next_time})")

            # 4️⃣ rename
            os.rename(temp_name, final_name)
            print(f"[완료됨] {final_name}")

        else:
            print(f"[건너뜀] {current_time.strftime('%H:%M:%S')}~{next_time.strftime('%H:%M:%S')} 데이터 없음")

        current_time = next_time
        time.sleep(drop_interval_sec)

    print("[완료] 모든 origin-data 파일 생성 완료 ✅")


def main():
    parser = argparse.ArgumentParser(description="Weather data simulator")
    parser.add_argument("--input", type=str, default="/app/data/kaggle/weather_data.csv")
    parser.add_argument("--output", type=str, default="/app/data/output")
    parser.add_argument("--drop_interval", type=int, default=int(os.getenv("drop_interval", 5)))
    parser.add_argument("--row_interval", type=int, default=int(os.getenv("row_interval", 60)))
    parser.add_argument("--error_rate", type=float, default=0.05)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    clean_output_dir(args.output)

    generate_origin_files(
        input_path=args.input,
        output_path=args.output,
        drop_interval_sec=args.drop_interval,
        row_interval_sec=args.row_interval,
        error_rate=args.error_rate
    )


if __name__ == "__main__":
    main()
