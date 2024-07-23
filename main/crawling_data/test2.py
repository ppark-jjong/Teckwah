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
    raw_data_file = "C:/MyMain/Teckwah/download/xlsx_files/test.xlsx"
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

    # Country 컬럼이 KR인 데이터만 필터링
    raw_df = raw_df[raw_df["Country"] == "KR"]

    return raw_df


def compare_data(raw_df, db_df):
    print(f"원본 데이터 형태: {raw_df.shape}")
    print(f"DB 데이터 형태: {db_df.shape}")

    if db_df.empty:
        print(
            "데이터베이스에서 데이터를 가져오지 못했습니다. 데이터베이스 연결과 테이블을 확인해주세요."
        )
        return

    db_df["composite_key"] = create_composite_key(db_df)

    # Cust_Sys_No 비교
    raw_unique_cust_sys_no = set(raw_df["Cust_Sys_No"].unique())
    db_cust_sys_no = set(db_df["Cust_Sys_No"])

    in_raw_not_in_db = raw_unique_cust_sys_no - db_cust_sys_no
    common_cust_sys_no = raw_unique_cust_sys_no.intersection(db_cust_sys_no)

    print(f"\n1. Cust_Sys_No 비교:")
    print(f"   원본 데이터의 유니크한 Cust_Sys_No 수: {len(raw_unique_cust_sys_no)}")
    print(f"   DB의 Cust_Sys_No 수: {len(db_cust_sys_no)}")
    print(
        f"   원본 데이터에는 있지만 DB에 없는 Cust_Sys_No 수: {len(in_raw_not_in_db)}"
    )
    print(f"   공통된 Cust_Sys_No 수: {len(common_cust_sys_no)}")

    match_rate = len(common_cust_sys_no) / len(raw_unique_cust_sys_no) * 100
    print(f"   원본 데이터의 Cust_Sys_No가 DB에 존재하는 비율: {match_rate:.2f}%")

    if in_raw_not_in_db:
        print("   원본 데이터에만 있는 Cust_Sys_No 샘플 (최대 5개):")
        print("   ", list(in_raw_not_in_db)[:5])

    # 원본 데이터의 중복 레코드 확인
    raw_duplicates = raw_df[raw_df.duplicated(subset="Cust_Sys_No", keep=False)]
    print(f"\n   원본 데이터의 중복된 Cust_Sys_No 수: {len(raw_duplicates)}")

    if not raw_duplicates.empty:
        print("   원본 데이터의 중복 Cust_Sys_No 샘플 (최대 5개):")
        print(raw_duplicates["Cust_Sys_No"].head())

    # CountPO 비교
    merged_df = pd.merge(
        raw_df[["composite_key", "Count_PO"]],
        db_df[["composite_key", "Count_PO"]],
        on="composite_key",
        suffixes=("_raw", "_db"),
    )
    count_match = (merged_df["Count_PO_raw"] == merged_df["Count_PO_db"]).sum()
    count_mismatch = len(merged_df) - count_match
    count_match_rate = count_match / len(merged_df) * 100 if len(merged_df) > 0 else 0

    print(f"\n2. CountPO 비교:")
    print(f"   CountPO 일치율: {count_match_rate:.2f}%")
    print(f"   CountPO 불일치 레코드 수: {count_mismatch}")

    if count_mismatch > 0:
        mismatch_keys = (
            merged_df[merged_df["Count_PO_raw"] != merged_df["Count_PO_db"]][
                "composite_key"
            ]
            .head(5)
            .tolist()
        )
        print("   CountPO 불일치 레코드 기본키 샘플 (최대 5개):")
        for key in mismatch_keys:
            print(f"     {key}")

    # Quantity 비교
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

    print(f"\n3. Quantity 비교:")
    print(f"   Quantity 일치율: {quantity_match_rate:.2f}%")
    print(f"   Quantity 불일치 레코드 수: {quantity_mismatch}")

    if quantity_mismatch > 0:
        mismatch_keys = (
            quantity_comparison[
                quantity_comparison["Quantity_raw"]
                != quantity_comparison["Quantity_db"]
            ]["composite_key"]
            .head(5)
            .tolist()
        )
        print("   Quantity 불일치 레코드 기본키 샘플 (최대 5개):")
        for key in mismatch_keys:
            print(f"     {key}")


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
