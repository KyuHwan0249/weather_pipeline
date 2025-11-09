import os
import time
import pandas as pd
import argparse
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
    print(f"[CLEANUP] {removed}개의 기존 CSV 파일 삭제 완료 ('.gitkeep'은 유지됨)")


def generate_origin_files(
    input_path: str,
    output_path: str,
    drop_interval_sec: int,
    row_interval_sec: int
):
    # CSV 읽기 및 정렬
    df = pd.read_csv(input_path)
    df['Date_Time'] = pd.to_datetime(df['Date_Time'])
    df = df.sort_values('Date_Time').reset_index(drop=True)

    # 0초 기준 정렬된 시간 범위 계산
    start_time = df['Date_Time'].min()
    start_time = start_time.replace(second=0, microsecond=0)  # ✅ 정각 기준으로 맞추기
    end_time = df['Date_Time'].max().replace(second=0, microsecond=0) + timedelta(minutes=1)

    print(f"[INFO] 총 {len(df)}행, 기간: {start_time} ~ {end_time}")
    print(f"[INFO] {drop_interval_sec}초마다 파일 생성, 데이터 간격 {row_interval_sec}초 단위 (정각 기준)")

    current_time = start_time

    while current_time <= end_time:
        next_time = current_time + timedelta(seconds=row_interval_sec)

        # row_interval_sec 구간 데이터 선택
        chunk = df[(df['Date_Time'] >= current_time) & (df['Date_Time'] < next_time)]

        if not chunk.empty:
            ts_str = current_time.strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(output_path, f"weather_{ts_str}.csv")
            chunk.to_csv(filename, index=False)
            print(f"[생성됨] {filename} ({len(chunk)} rows, {current_time}~{next_time})")
        else:
            print(f"[건너뜀] {current_time.strftime('%H:%M:%S')}~{next_time.strftime('%H:%M:%S')} 데이터 없음")

        current_time = next_time
        time.sleep(drop_interval_sec)

    print("[완료] 모든 origin-data 파일 생성 완료 ✅")


def main():
    parser = argparse.ArgumentParser(description="Weather data simulator")
    parser.add_argument("--input", type=str, default="/app/data/kaggle/weather_data.csv")
    parser.add_argument("--output", type=str, default="/app/data/output")
    parser.add_argument("--drop_interval", type=int, default=5, help="파일 생성 간격 (초)")
    parser.add_argument("--row_interval", type=int, default=60, help="데이터 시간 구간 (초)")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    # 실행 전 기존 CSV 정리
    clean_output_dir(args.output)

    # 시뮬레이션 실행
    generate_origin_files(
        input_path=args.input,
        output_path=args.output,
        drop_interval_sec=args.drop_interval,
        row_interval_sec=args.row_interval
    )


if __name__ == "__main__":
    main()
