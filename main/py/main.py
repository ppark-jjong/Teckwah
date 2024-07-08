import pandas as pd
from pyxlsb import open_workbook
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER
from file_handler import process_file
from database import create_tables, upload_to_mysql
from data_processor import process_dataframe


def read_xlsb(filepath, sheet_name):
    with open_workbook(filepath) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            data = [row for row in sheet.rows()]

    headers = [cell.v for cell in data[0]]
    data = [[cell.v for cell in row] for row in data[1:]]
    return pd.DataFrame(data, columns=headers)


def main():
    try:
        # 테이블 생성
        create_tables()

        # raw data 파일 경로
        raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"

        print("데이터 로드 중...")
        if raw_data_file.endswith(".xlsb"):
            df = read_xlsb(
                raw_data_file, "Receiving_TAT"
            )  # 'CS Receiving TAT'는 시트 이름입니다. 실제 시트 이름으로 변경해주세요.
        else:
            raise ValueError("지원되지 않는 파일 형식입니다.")

        print(f"로드된 데이터 행 수: {len(df)}")
        print("로드된 데이터 프레임의 열 이름:")
        print(df.columns.tolist())

        # 데이터 처리
        print("데이터 처리 중...")
        processed_df = process_dataframe(df)

        print("처리된 데이터 프레임의 열 이름:")
        print(processed_df.columns.tolist())

        # 데이터베이스에 업로드
        print("데이터베이스에 업로드 중...")
        upload_to_mysql(processed_df)

        print("데이터 처리 및 업로드 완료")

    except Exception as e:
        print(f"오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
