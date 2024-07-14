import pandas as pd
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER, DB_CONFIG, ORDER_TYPE_MAPPING, COLUMN_MAPPING
from file_handler import process_file
from database import create_tables, upload_to_mysql
from data_processor import main_data_processing

def read_excel(filepath, sheet_name):
    try:
        return pd.read_excel(filepath, sheet_name=sheet_name)
    except Exception as e:
        print(f"엑셀 파일 읽기 실패: {str(e)}")
        raise

def main():
    try:
        create_tables()

        raw_data_file = "C:/MyMain/Teckwah/download/xlsx_files_complete/200629_200705_ReceivingTAT_report.xlsx"
        print("데이터 로드 중...")
        
        df = read_excel(raw_data_file, "CS Receiving TAT")

        print(f"로드된 데이터 행 수: {len(df)}")
        print("로드된 데이터 프레임의 열 이름:")
        print(df.columns.tolist())

        print("데이터 처리 중...")
        config = {
            "DB_CONFIG": DB_CONFIG,
            "ORDER_TYPE_MAPPING": ORDER_TYPE_MAPPING,
            "COLUMN_MAPPING": COLUMN_MAPPING
        }
        processed_df = main_data_processing(df, config)

        print("처리된 데이터 프레임의 열 이름:")
        print(processed_df.columns.tolist())

        print("데이터베이스에 업로드 중...")
        upload_to_mysql(processed_df)

        print("데이터 처리 및 업로드 완료")

    except Exception as e:
        print(f"오류 발생: {str(e)}")

if __name__ == "__main__":
    main()