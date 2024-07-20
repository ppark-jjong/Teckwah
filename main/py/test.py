import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE
import pyxlsb


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
            print(f"DB 레코드 수: {len(df)}")
            return df
        else:
            print("DB 연결 실패")
            return pd.DataFrame()
    except Error as e:
        print(f"MySQL 연결 오류: {e}")
        return pd.DataFrame()


def preprocess_raw_data(raw_df):
    import pandas as pd


import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE
from datetime import datetime

# ... (이전 코드는 그대로 유지) ...


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

    # 중복 레코드 수 계산
    duplicate_count = raw_df.duplicated(subset="composite_key", keep=False).sum()
    print(f"\n원본 데이터의 중복 레코드 수: {duplicate_count}")

    if duplicate_count > 0:
        print("중복 레코드의 composite_key 샘플 (최대 5개):")
        duplicate_keys = raw_df[raw_df.duplicated(subset="composite_key", keep=False)][
            "composite_key"
        ].unique()[:5]
        for key in duplicate_keys:
            print(f"  {key}")

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

    # DB 데이터의 중복 레코드 수 계산
    db_duplicate_count = db_df.duplicated(subset="composite_key", keep=False).sum()
    print(f"\nDB 데이터의 중복 레코드 수: {db_duplicate_count}")

    if db_duplicate_count > 0:
        print("DB의 중복 레코드의 composite_key 샘플 (최대 5개):")
        db_duplicate_keys = db_df[db_df.duplicated(subset="composite_key", keep=False)][
            "composite_key"
        ].unique()[:5]
        for key in db_duplicate_keys:
            print(f"  {key}")

    # Cust_Sys_No 비교
    raw_cust_sys_no = set(raw_df["Cust_Sys_No"])
    db_cust_sys_no = set(db_df["Cust_Sys_No"])

    only_in_raw = raw_cust_sys_no - db_cust_sys_no
    only_in_db = db_cust_sys_no - raw_cust_sys_no

    print(f"\n1. Cust_Sys_No 비교:")
    print(f"   원본 데이터에만 있는 Cust_Sys_No 수: {len(only_in_raw)}")
    print(f"   DB에만 있는 Cust_Sys_No 수: {len(only_in_db)}")

    if only_in_raw:
        print("   원본 데이터에만 있는 Cust_Sys_No 샘플 (최대 5개):")
        print("   ", list(only_in_raw)[:5])

    if only_in_db:
        print("   DB에만 있는 Cust_Sys_No 샘플 (최대 5개):")
        print("   ", list(only_in_db)[:5])

    # ... (나머지 비교 로직은 그대로 유지) ...


# ... (main 함수와 나머지 코드는 그대로 유지) ...
def compare_cust_sys_no(raw_df, db_df):
    raw_cust_sys_no = set(raw_df["Cust_Sys_No"])
    db_cust_sys_no = set(db_df["Cust_Sys_No"])

    only_in_raw = raw_cust_sys_no - db_cust_sys_no
    only_in_db = db_cust_sys_no - raw_cust_sys_no

    print(f"\n1. Cust_Sys_No 비교:")
    print(f"   원본 데이터에만 있는 Cust_Sys_No 수: {len(only_in_raw)}")
    print(f"   DB에만 있는 Cust_Sys_No 수: {len(only_in_db)}")

    if only_in_raw:
        print("   원본 데이터에만 있는 Cust_Sys_No 샘플 (최대 5개):")
        print("   ", list(only_in_raw)[:5])

    if only_in_db:
        print("   DB에만 있는 Cust_Sys_No 샘플 (최대 5개):")
        print("   ", list(only_in_db)[:5])

    # 중복 확인
    raw_duplicates = raw_df[raw_df["Cust_Sys_No"].duplicated(keep=False)]
    db_duplicates = db_df[db_df["Cust_Sys_No"].duplicated(keep=False)]

    print(f"\n   원본 데이터의 중복된 Cust_Sys_No 수: {len(raw_duplicates)}")
    print(f"   DB의 중복된 Cust_Sys_No 수: {len(db_duplicates)}")

    if not raw_duplicates.empty:
        print("   원본 데이터의 중복 Cust_Sys_No 샘플 (최대 5개):")
        print(raw_duplicates["Cust_Sys_No"].head())

    if not db_duplicates.empty:
        print("   DB의 중복 Cust_Sys_No 샘플 (최대 5개):")
        print(db_duplicates["Cust_Sys_No"].head())


def compare_count_po(raw_df, db_df):
    merged_df = pd.merge(
        raw_df[["Cust_Sys_No", "Count_PO"]],
        db_df[["Cust_Sys_No", "Count_PO"]],
        on="Cust_Sys_No",
        suffixes=("_raw", "_db"),
    )
    count_mismatch = merged_df[merged_df["Count_PO_raw"] != merged_df["Count_PO_db"]]
    print(f"\n2. CountPO 비교:")
    print(f"   불일치 레코드 수: {len(count_mismatch)}")
    if not count_mismatch.empty:
        print("   CountPO 불일치 샘플 (최대 5개):")
        print(count_mismatch.head())


def compare_quantity(raw_df, db_df):
    quantity_comparison = pd.merge(
        raw_df[["Cust_Sys_No", "Quantity_Sum"]],
        db_df[["Cust_Sys_No", "Quantity"]],
        on="Cust_Sys_No",
    )
    quantity_mismatch = quantity_comparison[
        quantity_comparison["Quantity_Sum"] != quantity_comparison["Quantity"]
    ]
    print(f"\n3. Quantity 비교:")
    print(f"   불일치 레코드 수: {len(quantity_mismatch)}")
    if not quantity_mismatch.empty:
        print("   Quantity 불일치 샘플 (최대 5개):")
        print(quantity_mismatch.head())


def main():
    raw_df = get_raw_data()
    db_df = get_db_data()

    raw_df = preprocess_raw_data(raw_df)

    print(f"\n원본 데이터 레코드 수: {len(raw_df)}")
    print(f"DB 데이터 레코드 수: {len(db_df)}")

    compare_cust_sys_no(raw_df, db_df)
    compare_count_po(raw_df, db_df)
    compare_quantity(raw_df, db_df)


if __name__ == "__main__":
    main()
