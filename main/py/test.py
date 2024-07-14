import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE

def read_xlsb(filepath, sheet_name):
    import pyxlsb
    with pyxlsb.open_workbook(filepath) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            data = [[c.v for c in r] for r in sheet.rows()]
    return pd.DataFrame(data[1:], columns=data[0])

def get_raw_data():
    raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"
    return read_xlsb(raw_data_file, "Receiving_TAT")

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
    return df["ReceiptNo"].astype(str) + "|" + df["Replen_Balance_Order"].astype(str) + "|" + df["Cust_Sys_No"].astype(str)

def compare_data(raw_df, db_df):
    print(f"원본 데이터 형태: {raw_df.shape}")
    print(f"DB 데이터 형태: {db_df.shape}")

    if db_df.empty:
        print("데이터베이스에서 데이터를 가져오지 못했습니다. 데이터베이스 연결과 테이블을 확인해주세요.")
        return

    # 컬럼 이름 일치시키기
    raw_df = raw_df.rename(columns={
        "Replen/Balance Order#": "Replen_Balance_Order",
        "Cust Sys No": "Cust_Sys_No",
    })

    # 복합 키 생성
    raw_df["composite_key"] = create_composite_key(raw_df)
    db_df["composite_key"] = create_composite_key(db_df)

    # 원본 데이터에서 중복 제거 (첫 번째 행만 유지)
    raw_df_unique = raw_df.drop_duplicates(subset="composite_key", keep="first")

    # DB에 없는 원본 데이터 레코드 확인
    missing_in_db = raw_df_unique[~raw_df_unique["composite_key"].isin(db_df["composite_key"])]
    print(f"DB에 없는 원본 데이터 레코드 수: {len(missing_in_db)}")

    # 원본 데이터에 없는 DB 레코드 확인
    extra_in_db = db_df[~db_df["composite_key"].isin(raw_df_unique["composite_key"])]
    print(f"원본 데이터에 없는 DB 레코드 수: {len(extra_in_db)}")

    # CountPO 값 검증
    if "CountPO" in db_df.columns and "CountPO" in raw_df_unique.columns:
        db_df_with_raw = db_df.merge(raw_df_unique[["composite_key", "CountPO"]], on="composite_key", suffixes=("", "_raw"))
        incorrect_count = db_df_with_raw[db_df_with_raw["CountPO"] != db_df_with_raw["CountPO_raw"]]
        print(f"CountPO 값이 잘못된 레코드 수: {len(incorrect_count)}")

    # 샘플 데이터 출력
    if len(missing_in_db) > 0:
        print("\nDB에 없는 원본 데이터 샘플 (최대 5개):")
        print(missing_in_db[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head())

    if len(extra_in_db) > 0:
        print("\n원본 데이터에 없는 DB 레코드 샘플 (최대 5개):")
        print(extra_in_db[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head())

    if "CountPO" in db_df.columns and "CountPO" in raw_df_unique.columns and len(incorrect_count) > 0:
        print("\nCountPO 값이 잘못된 레코드 샘플 (최대 5개):")
        print(incorrect_count[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No", "CountPO", "CountPO_raw"]].head())

    # 열별 분석
    analyze_column_differences(raw_df_unique, db_df, 'Replen_Balance_Order')
    analyze_column_differences(raw_df_unique, db_df, 'PutAwayDate')

def analyze_column_differences(raw_df, db_df, column_name):
    print(f"\n'{column_name}' 열 분석:")
    
    # 데이터 타입 확인
    print(f"원본 데이터 타입: {raw_df[column_name].dtype}")
    print(f"DB 데이터 타입: {db_df[column_name].dtype}")
    
    # NULL 값 확인
    print(f"원본 데이터 NULL 값 수: {raw_df[column_name].isnull().sum()}")
    print(f"DB 데이터 NULL 값 수: {db_df[column_name].isnull().sum()}")
    
    # 유일한 값 비교
    raw_unique_values = set(raw_df[column_name].dropna().unique())
    db_unique_values = set(db_df[column_name].dropna().unique())
    
    lost_values = raw_unique_values - db_unique_values
    new_values = db_unique_values - raw_unique_values
    
    print(f"원본에만 있는 유일한 값의 수: {len(lost_values)}")
    print(f"DB에만 있는 유일한 값의 수: {len(new_values)}")

    # 샘플 데이터 출력
    if len(lost_values) > 0:
        print(f"\n원본에만 있는 값의 샘플 (최대 5개): {list(lost_values)[:5]}")
    if len(new_values) > 0:
        print(f"\nDB에만 있는 값의 샘플 (최대 5개): {list(new_values)[:5]}")

def main():
    raw_df = get_raw_data()
    db_df = get_db_data()
    compare_data(raw_df, db_df)

if __name__ == "__main__":
    main()