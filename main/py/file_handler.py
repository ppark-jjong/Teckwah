import os
import time
import shutil
import pandas as pd
import logging
from typing import Set, Optional
from config import DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)

def get_existing_files(folder: str) -> Set[str]:
    """
    지정된 폴더의 기존 파일 목록을 가져옵니다.
    """
    return set(os.listdir(folder))

def wait_for_download(
    download_folder: str, existing_files: Set[str], timeout: int = DOWNLOAD_TIMEOUT
) -> Set[str]:
    """
    새 파일이 다운로드될 때까지 대기합니다.
    """
    start_time = time.time()
    while True:
        current_files = set(os.listdir(download_folder))
        new_files = current_files - existing_files
        if any(file.endswith(".xlsx") for file in new_files):
            return new_files
        if time.time() - start_time > timeout:
            logger.warning("다운로드 대기 시간 초과")
            break
        time.sleep(1)
    return set()

def rename_downloaded_file(
    download_folder: str, new_files: Set[str], new_name: str
) -> Optional[str]:
    """
    다운로드된 파일의 이름을 변경합니다.
    """
    if new_files:
        latest_file = max(
            [
                os.path.join(download_folder, f)
                for f in new_files
                if f.endswith(".xlsx")
            ],
            key=os.path.getctime,
        )
        new_path = os.path.join(download_folder, new_name)
        os.rename(latest_file, new_path)
        logger.info(f"파일 이름이 {new_name}(으)로 변경되었습니다.")
        return new_path
    else:
        logger.warning("다운로드된 파일을 찾을 수 없습니다.")
        return None

def process_file(file_path: str, complete_folder: str, process_func) -> Optional[str]:
    """
    파일을 처리하고 완료 폴더로 이동합니다.
    """
    try:
        logger.info(f"파일 처리 시작: {file_path}")
        df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")
        logger.info("파일 로드 완료: 엑셀 파일을 데이터프레임으로 변환했습니다.")

        df = process_func(df)
        logger.info("데이터 프레임 변환 완료: 데이터 전처리를 완료했습니다.")

        new_file_path = os.path.join(complete_folder, os.path.basename(file_path))
        shutil.move(file_path, new_file_path)
        logger.info(
            f"파일 처리 완료: {os.path.basename(file_path)}을(를) 완료 폴더로 이동했습니다."
        )

        return new_file_path
    except Exception as e:
        logger.error(f"파일 처리 실패: {str(e)}")
        raise

def save_to_excel(df: pd.DataFrame, file_path: str) -> None:
    """
    데이터프레임을 엑셀 파일로 저장합니다.
    """
    try:
        df.to_excel(file_path, index=False)
        logger.info(f"데이터가 {file_path}에 성공적으로 저장되었습니다.")
    except Exception as e:
        logger.error(f"엑셀 파일 저장 중 오류 발생: {str(e)}")
        raise
