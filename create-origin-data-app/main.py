import os
import shutil

def copy_files(src_dir: str, dest_dir: str):
    """
    src_dir 경로의 모든 파일을 dest_dir로 복사합니다.
    동일한 이름의 파일이 있을 경우 덮어씁니다.
    """
    if not os.path.exists(src_dir):
        raise FileNotFoundError(f"소스 폴더가 존재하지 않습니다: {src_dir}")
    
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        print(f"📁 대상 폴더 생성: {dest_dir}")

    copied_files = []
    for file_name in os.listdir(src_dir):
        src_path = os.path.join(src_dir, file_name)
        dest_path = os.path.join(dest_dir, file_name)

        # 파일만 복사 (폴더는 무시)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dest_path)
            copied_files.append(file_name)
            print(f"✅ 복사 완료: {file_name}")

    if not copied_files:
        print("⚠️ 복사할 파일이 없습니다.")
    else:
        print(f"\n총 {len(copied_files)}개의 파일 복사 완료 ✅")

if __name__ == "__main__":
    # 컨테이너 내부 기준 경로
    src = "./data/kaggle"
    dest = "./data/output"

    print(f"🚀 파일 복사 시작: {src} → {dest}")
    copy_files(src, dest)
