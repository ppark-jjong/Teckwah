import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE
import pyxlsb
from datetime import datetime

def read_xlsb(filepath, sheet_name):
    with pyxlsb.open_workbook(filepath) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            data = [[c.v for c in r] for r in sheet.rows()]
    return pd.DataFrame(data[1:], columns=data[0])

def get_raw_data():
    raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"
    return read_xlsb(raw_data_file, "Receiving TAT")

def get_db_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}")
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            cursor.close()
            conn.close()
            print(f"데이터베이스에서 {len(df)}개의 레코드를 성공적으로 가져왔습니다.")
            return df
        else:
            print("데이터베이스 연결에 실패했습니다.")
            return pd.DataFrame()
    except Error as e:
        print(f"MySQL 연결 중 오류 발생: {e}")
        return pd.DataFrame()

def create_composite_key(df):
    return (
        df["ReceiptNo"].astype(str)
        + "|"
        + df["Replen_Balance_Order"].astype(str)
        + "|"
        + df["Cust_Sys_No"].astype(str)
    )

def preprocess_raw_data(raw_df):
    raw_df = raw_df.rename(
        columns={
            "Replen/Balance Order#": "Replen_Balance_Order",
            "Cust Sys No": "Cust_Sys_No",
        }
    )

    raw_df["Replen_Balance_Order"] = raw_df["Replen_Balance_Order"].astype(str)
    raw_df["Replen_Balance_Order"] = raw_df["Replen_Balance_Order"].apply(
        lambda x: x.split(".")[0] if "." in x else x
    )

    raw_df["composite_key"] = create_composite_key(raw_df)
    raw_df["Count_PO"] = raw_df.groupby("composite_key")["composite_key"].transform("count")
    return raw_df

def compare_data(raw_df, db_df):
    print(f"원본 데이터 형태: {raw_df.shape}")
    print(f"DB 데이터 형태: {db_df.shape}")

    if db_df.empty:
        print("데이터베이스에서 데이터를 가져오지 못했습니다. 데이터베이스 연결과 테이블을 확인해주세요.")
        return

    raw_df = preprocess_raw_data(raw_df)
    db_df["composite_key"] = create_composite_key(db_df)

    db_keys = set(db_df["composite_key"])
    raw_keys = set(raw_df["composite_key"])
    missing_in_db = raw_keys - db_keys
    extra_in_db = db_keys - raw_keys

    print(f"DB에 없는 원본 데이터 키 수: {len(missing_in_db)}")
    print(f"원본 데이터에 없는 DB 키 수: {len(extra_in_db)}")

    print(f"\nReplen_Balance_Order 데이터 타입:")
    print(f"원본 데이터: {raw_df['Replen_Balance_Order'].dtype}")
    print(f"DB 데이터: {db_df['Replen_Balance_Order'].dtype}")

    raw_replen = set(raw_df["Replen_Balance_Order"])
    db_replen = set(db_df["Replen_Balance_Order"])

    replen_mismatch = raw_replen.symmetric_difference(db_replen)
    print(f"\nReplen_Balance_Order 불일치 값 수: {len(replen_mismatch)}")

    if replen_mismatch:
        print("Replen_Balance_Order 불일치 값 샘플 (최대 5개):")
        print(list(replen_mismatch)[:5])

    merged_df = pd.merge(
        raw_df.groupby("composite_key").agg({"Count_PO": "first"}),
        db_df[["composite_key", "Count_PO"]],
        on="composite_key",
        suffixes=("_raw", "_db"),
    )
    count_mismatch = merged_df[merged_df["Count_PO_raw"] != merged_df["Count_PO_db"]]
    print(f"CountPO 불일치 레코드 수: {len(count_mismatch)}")

    raw_quantity = raw_df.groupby("composite_key")["Quantity"].sum().reset_index()
    db_quantity = db_df[["composite_key", "Quantity"]]
    quantity_comparison = pd.merge(
        raw_quantity, db_quantity, on="composite_key", suffixes=("_raw", "_db")
    )
    quantity_mismatch = quantity_comparison[
        quantity_comparison["Quantity_raw"] != quantity_comparison["Quantity_db"]
    ]
    print(f"Quantity 합계 불일치 레코드 수: {len(quantity_mismatch)}")

    if len(missing_in_db) > 0:
        print("\nDB에 없는 원본 데이터 키 샘플 (최대 5개):")
        print(list(missing_in_db)[:5])

    if len(extra_in_db) > 0:
        print("\n원본 데이터에 없는 DB 키 샘플 (최대 5개):")
        print(list(extra_in_db)[:5])

    if len(count_mismatch) > 0:
        print("\nCountPO 불일치 샘플 (최대 5개):")
        print(count_mismatch.head())

    if len(quantity_mismatch) > 0:
        print("\nQuantity 합계 불일치 샘플 (최대 5개):")
        print(quantity_mismatch.head())

def main():
    raw_df = get_raw_data()
    db_df = get_db_data()
    compare_data(raw_df, db_df)

if __name__ == "__main__":
    main()