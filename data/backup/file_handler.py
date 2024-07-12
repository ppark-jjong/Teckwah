import os
import time
import datetime
import shutil
import pandas as pd
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER, DOWNLOAD_TIMEOUT
from data_processor import process_dataframe
from database import upload_to_mysql

def get_existing_files(folder):
    return set(os.listdir(folder))

def wait_for_download(download_folder, existing_files, timeout=DOWNLOAD_TIMEOUT):
    start_time = time.time()
    while True:
        current_files = set(os.listdir(download_folder))
        new_files = current_files - existing_files
        if any(file.endswith(".xlsx") for file in new_files):
            return new_files
        if time.time() - start_time > timeout:
            print("다운로드 대기 시간 초과")
            break
        time.sleep(1)
    return set()

def rename_downloaded_file(download_folder, new_files, new_name):
    if new_files:
        latest_file = max(
            [os.path.join(download_folder, f) for f in new_files if f.endswith(".xlsx")],
            key=os.path.getctime,
        )
        new_path = os.path.join(download_folder, new_name)
        os.rename(latest_file, new_path)
        print(f"파일 이름이 {new_name}(으)로 변경되었습니다.")
        return new_path
    else:
        print("다운로드된 파일을 찾을 수 없습니다.")
        return None

def process_file(file_path):
    try:
        print(f"파일 처리 시작: {file_path}")
        df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")
        print("파일 로드 완료: 엑셀 파일을 데이터프레임으로 변환했습니다.")
        df = process_dataframe(df)
        print("데이터 프레임 변환 완료: 데이터 전처리를 완료했습니다.")
        upload_to_mysql(df)
        print("데이터베이스 업로드 완료: MySQL 데이터베이스에 데이터를 업로드했습니다.")
        
        # 파일을 완료 폴더로 이동
        new_file_path = os.path.join(COMPLETE_FOLDER, os.path.basename(file_path))
        shutil.move(file_path, new_file_path)
        print(f"파일 처리 완료: {os.path.basename(file_path)}을(를) 완료 폴더로 이동했습니다.")
        
        return new_file_path  # 새 파일 경로 반환
    except Exception as e:
        print(f"파일 처리 실패: {str(e)}")
        raise

def format_date_yy_mm_dd(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")