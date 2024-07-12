import os
import logging
from typing import Dict, Any
import pandas as pd
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER, DB_CONFIG, COLUMN_MAPPING, ORDER_TYPE_MAPPING
from web_crawler import initialize_and_login, WebCrawler
from file_handler import get_existing_files, wait_for_download, rename_downloaded_file, process_file
from database import create_tables, upload_to_mysql
from data_processor import main_data_processing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    config: Dict[str, Any] = {
        'DOWNLOAD_FOLDER': DOWNLOAD_FOLDER,
        'COMPLETE_FOLDER': COMPLETE_FOLDER,
        'DB_CONFIG': DB_CONFIG,
        'COLUMN_MAPPING': COLUMN_MAPPING,
        'ORDER_TYPE_MAPPING': ORDER_TYPE_MAPPING
    }

    crawler: Optional[WebCrawler] = None
    try:
        username = "jhypark-dir"
        password = "Hyeok970209!"
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        # 웹 크롤러 초기화 및 로그인
        crawler = initialize_and_login(config, username, password)

        # 기존 파일 목록 저장
        existing_files = get_existing_files(DOWNLOAD_FOLDER)

        # 새 파일 이름 지정
        new_name = f"{start_date[:2]}{start_date[5:7]}{start_date[8:]}_{end_date[:2]}{end_date[5:7]}{end_date[8:]}_ReceivingTAT_report.xlsx"

        # 지정된 report 찾기 및 다운로드 실행
        logger.info("Report를 찾고 다운로드를 시작합니다.")
        crawler.process_rma_return(start_date, end_date)

        # 파일 다운로드 대기
        new_files = wait_for_download(DOWNLOAD_FOLDER, existing_files)

        # 다운로드된 파일의 이름 변경
        logger.info("다운로드된 파일의 이름을 변경합니다.")
        file_path = rename_downloaded_file(DOWNLOAD_FOLDER, new_files, new_name)

        if file_path:
            # 테이블 생성
            create_tables()

            # 다운로드된 파일 처리
            logger.info("다운로드된 파일을 처리합니다.")
            new_file_path = process_file(file_path, COMPLETE_FOLDER, lambda df: main_data_processing(df, config))

            if new_file_path:
                # 처리된 데이터를 데이터베이스에 업로드
                df = pd.read_excel(new_file_path)
                upload_to_mysql(df)
                logger.info("데이터가 성공적으로 데이터베이스에 업로드되었습니다.")
            else:
                logger.error("파일 처리에 실패했습니다.")
        else:
            logger.error("파일 다운로드에 실패했습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}")
    finally:
        if crawler:
            crawler.close()
            logger.info("웹 크롤러를 종료합니다.")

if __name__ == "__main__":
    main()