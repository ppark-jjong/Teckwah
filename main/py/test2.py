import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE
from datetime import datetime


def read_excel(filepath, sheet_name):
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        print(f"Successfully read {len(df)} rows from the Excel file.")
        return df
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame()


def get_raw_data():
    raw_data_file = (
        "C:/MyMain/Teckwah/download/xlsx_files/240622_240628_ReceivingTAT_report.xlsx"
    )
    return read_excel(raw_data_file, "CS Receiving TAT")


def get_db_data(start_date, end_date):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            query = f"""
            SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}
            WHERE InventoryDate BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))
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
            "PutAwayDate": "InventoryDate",
        }
    )

    raw_df["Replen_Balance_Order"] = raw_df["Replen_Balance_Order"].astype(str)
    raw_df["Replen_Balance_Order"] = raw_df["Replen_Balance_Order"].apply(
        lambda x: x.split(".")[0] if "." in x else x
    )

    raw_df["InventoryDate"] = pd.to_datetime(raw_df["InventoryDate"]).dt.date
    raw_df["composite_key"] = create_composite_key(raw_df)
    raw_df["Count_PO"] = raw_df.groupby("composite_key")["composite_key"].transform(
        "count"
    )
    return raw_df


def compare_data(raw_df, db_df):
    print(f"원본 데이터 형태: {raw_df.shape}")
    print(f"DB 데이터 형태: {db_df.shape}")

    if db_df.empty:
        print(
            "데이터베이스에서 데이터를 가져오지 못했습니다. 데이터베이스 연결과 테이블을 확인해주세요."
        )
        return

    raw_df = preprocess_raw_data(raw_df)
    db_df["composite_key"] = create_composite_key(db_df)

    # CountPO 일치율 계산
    merged_df = pd.merge(
        raw_df[["composite_key", "Count_PO"]],
        db_df[["composite_key", "Count_PO"]],
        on="composite_key",
        suffixes=("_raw", "_db"),
    )
    count_match = (merged_df["Count_PO_raw"] == merged_df["Count_PO_db"]).sum()
    count_mismatch = len(merged_df) - count_match
    count_match_rate = count_match / len(merged_df) * 100 if len(merged_df) > 0 else 0

    print(f"\nCountPO 일치율: {count_match_rate:.2f}%")
    print(f"CountPO 불일치 레코드 수: {count_mismatch}")

    if count_mismatch > 0:
        mismatch_keys = (
            merged_df[merged_df["Count_PO_raw"] != merged_df["Count_PO_db"]][
                "composite_key"
            ]
            .head(5)
            .tolist()
        )
        print("CountPO 불일치 레코드 기본키 샘플 (최대 5개):")
        for key in mismatch_keys:
            print(f"  {key}")

    # Quantity 일치율 계산
    raw_quantity = raw_df.groupby("composite_key")["Quantity"].sum().reset_index()
    db_quantity = db_df.groupby("composite_key")["Quantity"].sum().reset_index()
    quantity_comparison = pd.merge(
        raw_quantity, db_quantity, on="composite_key", suffixes=("_raw", "_db")
    )
    quantity_match = (
        quantity_comparison["Quantity_raw"] == quantity_comparison["Quantity_db"]
    ).sum()
    quantity_mismatch = len(quantity_comparison) - quantity_match
    quantity_match_rate = (
        quantity_match / len(quantity_comparison) * 100
        if len(quantity_comparison) > 0
        else 0
    )

    print(f"\nQuantity 일치율: {quantity_match_rate:.2f}%")
    print(f"Quantity 불일치 레코드 수: {quantity_mismatch}")

    if quantity_mismatch > 0:
        mismatch_keys = (
            quantity_comparison[
                quantity_comparison["Quantity_raw"]
                != quantity_comparison["Quantity_db"]
            ]["composite_key"]
            .head(5)
            .tolist()
        )
        print("Quantity 불일치 레코드 기본키 샘플 (최대 5개):")
        for key in mismatch_keys:
            print(f"  {key}")

    # Cust_Sys_No 일치율 계산
    raw_cust = raw_df[["composite_key", "Cust_Sys_No"]].drop_duplicates()
    db_cust = db_df[["composite_key", "Cust_Sys_No"]].drop_duplicates()
    cust_comparison = pd.merge(
        raw_cust, db_cust, on="composite_key", suffixes=("_raw", "_db")
    )
    cust_match = (
        cust_comparison["Cust_Sys_No_raw"] == cust_comparison["Cust_Sys_No_db"]
    ).sum()
    cust_mismatch = len(cust_comparison) - cust_match
    cust_match_rate = (
        cust_match / len(cust_comparison) * 100 if len(cust_comparison) > 0 else 0
    )

    print(f"\nCust_Sys_No 일치율: {cust_match_rate:.2f}%")
    print(f"Cust_Sys_No 불일치 레코드 수: {cust_mismatch}")

    if cust_mismatch > 0:
        mismatch_keys = (
            cust_comparison[
                cust_comparison["Cust_Sys_No_raw"] != cust_comparison["Cust_Sys_No_db"]
            ]["composite_key"]
            .head(5)
            .tolist()
        )
        print("Cust_Sys_No 불일치 레코드 기본키 샘플 (최대 5개):")
        for key in mismatch_keys:
            print(f"  {key}")


def main():
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")

    raw_df = get_raw_data()
    if raw_df.empty:
        print("Raw data could not be read. Please check the file path and format.")
        return

    db_df = get_db_data(start_date, end_date)

    # raw_df를 입력받은 기간에 맞게 필터링
    raw_df = preprocess_raw_data(raw_df)
    raw_df = raw_df[
        (raw_df["InventoryDate"] >= datetime.strptime(start_date, "%Y-%m-%d").date())
        & (raw_df["InventoryDate"] <= datetime.strptime(end_date, "%Y-%m-%d").date())
    ]

    compare_data(raw_df, db_df)


if __name__ == "__main__":
    main()
