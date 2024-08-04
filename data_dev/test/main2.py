import os
import logging
from typing import Dict, Any
import warnings
import pandas as pd
from config import (
    DOWNLOAD_FOLDER,
    COMPLETE_FOLDER,
    DB_CONFIG,
    COLUMN_MAPPING,
    ORDER_TYPE_MAPPING,
)
from database import create_tables, upload_to_mysql
from data_processor import main_data_processing

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)
logger = logging.getLogger(__name__)


def setup_config() -> Dict[str, Any]:
    """
    설정 정보를 반환합니다.
    """
    return {
        "DOWNLOAD_FOLDER": DOWNLOAD_FOLDER,
        "COMPLETE_FOLDER": COMPLETE_FOLDER,
        "DB_CONFIG": DB_CONFIG,
        "COLUMN_MAPPING": COLUMN_MAPPING,
        "ORDER_TYPE_MAPPING": ORDER_TYPE_MAPPING,
    }


def get_user_input() -> str:
    """
    사용자로부터 파일 이름을 입력받습니다.
    """
    return input("처리할 파일 이름을 입력하세요 (확장자 포함): ")


def read_excel(filepath: str, sheet_name: str) -> pd.DataFrame:
    """
    엑셀 파일을 읽어 데이터프레임으로 반환합니다.
    """
    try:
        print(f"엑셀 파일 '{filepath}' 읽기 시작")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            df = pd.read_excel(filepath, sheet_name=sheet_name)
        print(f"엑셀 파일에서 {len(df)} 행의 데이터를 읽었습니다.")
        return df
    except Exception as e:
        print(f"엑셀 파일 읽기 실패: {str(e)}")
        raise


def process_and_upload_data(file_path: str, config: Dict[str, Any]):
    try:
        print(f"\n{'='*50}")
        print(f"파일 '{os.path.basename(file_path)}' 처리 시작")
        print(f"{'='*50}")

        df = read_excel(file_path, "CS Receiving TAT")
        print(f"\n원본 데이터 행 수: {len(df)}")
        print(f"원본 데이터 열: {', '.join(df.columns.tolist())}")

        print("\n데이터 처리 중...")
        processed_df, stats = main_data_processing(df, config)

        print(f"\n처리된 데이터 행 수: {len(processed_df)}")
        print(f"처리된 데이터 열: {', '.join(processed_df.columns.tolist())}")

        print("\n데이터베이스에 업로드 중...")
        upload_to_mysql(processed_df)
        print("데이터베이스 업로드 완료")

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        logger.error(f"파일 처리 중 오류 발생: {str(e)}", exc_info=True)


def main():
    """
    메인 함수: 설정 초기화, 사용자 입력 수집, 파일 처리 및 데이터베이스 업로드
    """
    config = setup_config()

    try:
        print("\n엑셀 파일 데이터 변환 프로그램")
        print("=" * 40)
        file_name = get_user_input()
        file_path = os.path.join(DOWNLOAD_FOLDER, file_name)

        if os.path.exists(file_path):
            print("\n테이블 생성 중...")
            create_tables()
            print("테이블 생성 완료")

            process_and_upload_data(file_path, config)

            print("\n처리 완료!")
        else:
            print(f"\n오류: 파일을 찾을 수 없습니다 - {file_path}")

    except Exception as e:
        print(f"\n예상치 못한 오류 발생: {str(e)}")
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
