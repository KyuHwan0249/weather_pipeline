import os
import time
import pandas as pd
import argparse
import uuid
from datetime import datetime, timedelta


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
    row_interval_sec: int
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
            # 1️⃣ 임시 이름은 uuid (확장자 없음)
            temp_name = os.path.join(output_path, str(uuid.uuid4()))

            # 2️⃣ 최종 이름은 기존 규칙 weather_YYYYMMDD_HHMMSS.csv
            ts_str = current_time.strftime("%Y%m%d_%H%M%S")
            final_name = os.path.join(output_path, f"weather_{ts_str}.csv")

            # 3️⃣ 데이터 저장
            chunk.to_csv(temp_name, index=False)
            print(f"[작성중] {temp_name} ({len(chunk)} rows, {current_time}~{next_time})")

            # 4️⃣ 저장 완료 후 rename
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
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    clean_output_dir(args.output)

    generate_origin_files(
        input_path=args.input,
        output_path=args.output,
        drop_interval_sec=args.drop_interval,
        row_interval_sec=args.row_interval
    )


if __name__ == "__main__":
    main()
