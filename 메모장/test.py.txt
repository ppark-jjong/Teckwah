import pandas as pd
import mysql.connector
from pyxlsb import open_workbook
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE
from data_processor import DataProcessor
from database import MySQLConnectionPool, get_db_data
import datetime


def load_original_data(file_path):
    with open_workbook(file_path) as wb:
        with wb.get_sheet("Receiving_TAT") as sheet:
            data = [row for row in sheet.rows()]

    headers = [cell.v for cell in data[0]]
    df = pd.DataFrame([[cell.v for cell in row] for row in data[1:]], columns=headers)
    return df


def test_data_integrity():
    raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"
    original_df = load_original_data(raw_data_file)

    print("데이터베이스 연결 테스트 시작...")
    try:
        with MySQLConnectionPool() as conn:
            conn.execute_query("SELECT 1")
            print("데이터베이스 연결 성공")
    except Exception as e:
        print(f"데이터베이스 연결 실패: {str(e)}")

    db_df = get_db_data()
    if db_df.empty:
        print("경고: 데이터베이스에서 데이터를 가져오지 못했습니다.")
    else:
        print(f"데이터베이스에서 {len(db_df)} 개의 행을 가져왔습니다.")

    # 1. 기본키 기준 누락 데이터 확인
    original_keys = set(
        zip(
            original_df["ReceiptNo"],
            original_df["Replen/Balance Order#"],
            original_df["Cust Sys No"],
        )
    )
    db_keys = (
        set(
            zip(db_df["ReceiptNo"], db_df["Replen_Balance_Order"], db_df["Cust_Sys_No"])
        )
        if not db_df.empty
        else set()
    )

    missing_keys = original_keys - db_keys
    print(f"누락된 데이터 개수: {len(missing_keys)}")

    # 2. Count_PO 확인
    if not db_df.empty:
        original_count = (
            original_df.groupby(["ReceiptNo", "Replen/Balance Order#", "Cust Sys No"])
            .size()
            .reset_index(name="Original_Count")
        )
        db_count = db_df[
            ["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No", "Count_PO"]
        ]

        merged_count = pd.merge(
            original_count,
            db_count,
            left_on=["ReceiptNo", "Replen/Balance Order#", "Cust Sys No"],
            right_on=["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"],
        )
        incorrect_count = merged_count[
            merged_count["Original_Count"] != merged_count["Count_PO"]
        ]

        print(f"Count_PO가 잘못 계산된 데이터 개수: {len(incorrect_count)}")
    else:
        print("Count_PO 확인 불가: 데이터베이스에서 데이터를 가져오지 못했습니다.")

    # 3. 회계년, 월, 주 계산 확인
    if not db_df.empty:

        def calculate_fiscal_data(date):
            processor = DataProcessor({})
            fy, week = processor.get_dell_week_and_fy(date)
            quarter = processor.get_quarter(date)
            return fy, week, quarter

        incorrect_fiscal = []
        for _, row in db_df.iterrows():
            date = row["PutAwayDate"].date()
            calculated_fy, calculated_week, calculated_quarter = calculate_fiscal_data(
                date
            )
            if (
                calculated_fy != row["FY"]
                or calculated_week != row["Week"]
                or calculated_quarter != row["Quarter"]
            ):
                incorrect_fiscal.append(row["ReceiptNo"])

        print(f"회계년, 주, 분기가 잘못 계산된 데이터 개수: {len(incorrect_fiscal)}")
    else:
        print("회계 데이터 확인 불가: 데이터베이스에서 데이터를 가져오지 못했습니다.")

    # 결과 요약
    print("\n테스트 결과 요약:")
    print(f"전체 원본 데이터 개수: {len(original_df)}")
    print(f"전체 DB 데이터 개수: {len(db_df)}")
    print(f"누락된 데이터 비율: {len(missing_keys) / len(original_df) * 100:.2f}%")
    if not db_df.empty:
        print(f"Count_PO 오류 비율: {len(incorrect_count) / len(db_df) * 100:.2f}%")
        print(f"회계 데이터 오류 비율: {len(incorrect_fiscal) / len(db_df) * 100:.2f}%")
    else:
        print(
            "데이터베이스에서 데이터를 가져오지 못해 추가적인 오류 비율을 계산할 수 없습니다."
        )


if __name__ == "__main__":
    test_data_integrity()
