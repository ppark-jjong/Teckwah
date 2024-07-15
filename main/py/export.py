import logging
import os
import pandas as pd
from datetime import datetime
from database import get_data_by_inventory_date
from file_handler import save_to_excel
from config import COMPLETE_FOLDER  # COMPLETE_FOLDER를 임포트

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="extract_data.log",
    filemode="a",
)
logger = logging.getLogger(__name__)

def get_user_input() -> tuple:
    """
    사용자로부터 시작 날짜와 종료 날짜를 입력받습니다.

    :return: 시작 날짜와 종료 날짜
    """
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    return start_date, end_date

def format_date(date_str: str) -> str:
    """
    날짜 문자열을 'YYMMDD' 형식으로 변환합니다.

    :param date_str: 날짜 문자열 (YYYY-MM-DD)
    :return: 변환된 날짜 문자열 (YYMMDD)
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.strftime("%y%m%d")

def main():
    try:
        start_date, end_date = get_user_input()

        # 입력 날짜 유효성 검사
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            logger.error("날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식으로 입력해주세요.")
            return

        # 데이터베이스에서 데이터 추출
        logger.info(f"데이터베이스에서 {start_date}부터 {end_date}까지의 데이터를 추출합니다.")
        df = get_data_by_inventory_date(start_date, end_date)
        logger.info(f"총 {len(df)}개의 데이터가 추출되었습니다.")

        # 엑셀 파일로 저장
        if not df.empty:
            formatted_start_date = format_date(start_date)
            formatted_end_date = format_date(end_date)
            file_name = f"{formatted_start_date}_{formatted_end_date}_ReceivingTAT_report.xlsx"
            file_path = os.path.join(COMPLETE_FOLDER, file_name)
            save_to_excel(df, file_path)
            logger.info(f"데이터가 엑셀 파일로 저장되었습니다: {file_path}")
        else:
            logger.info("지정된 기간 동안의 데이터가 없습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
