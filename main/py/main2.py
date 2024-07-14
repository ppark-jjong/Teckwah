import os
import logging
from typing import Dict, Any, Optional
import pandas as pd
from config import (
    DOWNLOAD_FOLDER,
    COMPLETE_FOLDER,
    DB_CONFIG,
    COLUMN_MAPPING,
    ORDER_TYPE_MAPPING,
)
from web_crawler import initialize_and_login, WebCrawler
from file_handler import (
    get_existing_files,
    wait_for_download,
    rename_downloaded_file,
)
from database import create_tables, upload_to_mysql
from data_processor import main_data_processing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)
logger = logging.getLogger(__name__)


def setup_config() -> Dict[str, Any]:
    return {
        "DOWNLOAD_FOLDER": DOWNLOAD_FOLDER,
        "COMPLETE_FOLDER": COMPLETE_FOLDER,
        "DB_CONFIG": DB_CONFIG,
        "COLUMN_MAPPING": COLUMN_MAPPING,
        "ORDER_TYPE_MAPPING": ORDER_TYPE_MAPPING,
    }


def get_user_input() -> tuple:
    username = "jhypark-dir"
    password = "Hyeok970209!"
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    return username, password, start_date, end_date


def read_excel(filepath: str, sheet_name: str) -> pd.DataFrame:
    try:
        logger.info(f"엑셀 파일 '{filepath}' 읽기 시작")
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        logger.info(f"엑셀 파일에서 {len(df)} 행의 데이터를 읽었습니다.")
        return df
    except Exception as e:
        logger.error(f"엑셀 파일 읽기 실패: {str(e)}")
        raise


def process_and_upload_data(file_path: str, config: Dict[str, Any]):
    try:
        logger.info(f"파일 '{file_path}' 처리 시작")
        df = read_excel(file_path, "CS Receiving TAT")

        logger.info("데이터 처리 중...")
        processed_df = main_data_processing(df, config)
        logger.info(f"데이터 처리 완료. 처리된 데이터 행 수: {len(processed_df)}")

        logger.info("처리된 데이터 프레임의 열 이름:")
        logger.info(processed_df.columns.tolist())

        logger.info("데이터베이스에 업로드 중...")
        upload_to_mysql(processed_df)
        logger.info("데이터가 성공적으로 데이터베이스에 업로드되었습니다.")
    except Exception as e:
        logger.error(f"파일 처리 중 오류 발생: {str(e)}", exc_info=True)


def main():
    config = setup_config()
    crawler: Optional[WebCrawler] = None

    try:
        username, password, start_date, end_date = get_user_input()

        crawler = initialize_and_login(config, username, password)

        existing_files = get_existing_files(DOWNLOAD_FOLDER)

        new_name = f"{start_date[:2]}{start_date[5:7]}{start_date[8:]}_{end_date[:2]}{end_date[5:7]}{end_date[8:]}_ReceivingTAT_report.xlsx"

        logger.info("Report를 찾고 다운로드를 시작합니다.")
        crawler.process_rma_return(start_date, end_date)

        new_files = wait_for_download(DOWNLOAD_FOLDER, existing_files)

        logger.info("다운로드된 파일의 이름을 변경합니다.")
        file_path = rename_downloaded_file(DOWNLOAD_FOLDER, new_files, new_name)

        if file_path:
            create_tables()
            logger.info("다운로드된 파일을 처리합니다.")
            process_and_upload_data(file_path, config)
        else:
            logger.error("파일 다운로드에 실패했습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)
    finally:
        if crawler:
            crawler.close()
            logger.info("웹 크롤러를 종료합니다.")


if __name__ == "__main__":
    main()
