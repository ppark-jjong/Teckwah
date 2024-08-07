import pandas as pd
from pyxlsb import open_workbook
from config import (
    DB_CONFIG,
    ORDER_TYPE_MAPPING,
    COLUMN_MAPPING,
    DOWNLOAD_FOLDER,
    COMPLETE_FOLDER,
)
from file_handler import process_file
from database import create_tables, upload_to_mysql
from data_processor import DataProcessor, main_data_processing

# Data Raw 파일 => db 변환 main 파일 아래 6개 파이썬 파일 import 연동 중
# config.py: 설정 정보를 저장
# database.py: 데이터베이스 관련 기능
# web_crawler.py: 웹 크롤링 관련 기능
# data_processor.py: 데이터 처리 관련 기능
# file_handler.py: 파일 처리 관련 기능
# main.py: 메인 실행 스크립트
# login_crawling.py : 크롤링을 위한 처리 관련 기능
# test.py : 원본데이터와 DB데이터 비교 테스트 기능


def read_xlsb(filepath, sheet_name):
    """
    xlsb 파일을 읽어 데이터프레임으로 변환합니다.
    """
    with open_workbook(filepath) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            data = [row for row in sheet.rows()]

    headers = [cell.v for cell in data[0]]
    data = [[cell.v for cell in row] for row in data[1:]]
    return pd.DataFrame(data, columns=headers)


def main():
    try:
        create_tables()

        raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"
        print("데이터 로드 중...")
        if raw_data_file.endswith(".xlsb"):
            df = read_xlsb(raw_data_file, "Receiving TAT")
        else:
            raise ValueError("지원되지 않는 파일 형식입니다.")

        print(f"로드된 데이터 행 수: {len(df)}")
        print("로드된 데이터 프레임의 열 이름:")
        print(df.columns.tolist())

        print("데이터 처리 중...")
        config = {
            "DB_CONFIG": DB_CONFIG,
            "ORDER_TYPE_MAPPING": ORDER_TYPE_MAPPING,
            "COLUMN_MAPPING": COLUMN_MAPPING,
        }
        processed_df = main_data_processing(df, config)

        print("데이터베이스에 업로드 중...")
        upload_to_mysql(processed_df)

        print("데이터 처리 및 업로드 완료")

    except Exception as e:
        print(f"오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
