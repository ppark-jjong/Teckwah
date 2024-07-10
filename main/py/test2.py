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

def calculate_count_po(df):
    return df.groupby('composite_key').size().reset_index(name='Calculated_Count_PO')

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

    # 원본 데이터에서 Count_PO 계산
    raw_count_po = calculate_count_po(raw_df)

    # DB 데이터와 계산된 Count_PO 비교
    merged_df = db_df.merge(raw_count_po, on='composite_key', how='outer', indicator=True)

    # Count_PO 비교
    if 'Count_PO' in db_df.columns:
        merged_df['Count_PO_Match'] = merged_df['Count_PO'] == merged_df['Calculated_Count_PO']
        print(f"\nCount_PO 분석:")
        print(f"전체 레코드 수: {len(merged_df)}")
        print(f"Count_PO 일치 레코드 수: {merged_df['Count_PO_Match'].sum()}")
        print(f"Count_PO 불일치 레코드 수: {(~merged_df['Count_PO_Match']).sum()}")

        # 불일치 샘플 출력
        mismatched = merged_df[~merged_df['Count_PO_Match']]
        if len(mismatched) > 0:
            print("\nCount_PO 불일치 샘플 (최대 5개):")
            print(mismatched[['ReceiptNo', 'Replen_Balance_Order', 'Cust_Sys_No', 'Count_PO', 'Calculated_Count_PO']].head())

        # Count_PO 통계
        print("\nCount_PO 통계:")
        print(f"DB Count_PO 평균: {db_df['Count_PO'].mean():.2f}")
        print(f"계산된 Count_PO 평균: {merged_df['Calculated_Count_PO'].mean():.2f}")
        print(f"DB Count_PO 최대값: {db_df['Count_PO'].max()}")
        print(f"계산된 Count_PO 최대값: {merged_df['Calculated_Count_PO'].max()}")

        # 큰 차이가 나는 경우 확인
        large_diff = merged_df[abs(merged_df['Count_PO'] - merged_df['Calculated_Count_PO']) > 5]
        if len(large_diff) > 0:
            print("\nCount_PO 큰 차이 (>5) 샘플 (최대 5개):")
            print(large_diff[['ReceiptNo', 'Replen_Balance_Order', 'Cust_Sys_No', 'Count_PO', 'Calculated_Count_PO']].head())
    else:
        print("\nDB에 Count_PO 열이 없습니다. Count_PO 분석을 건너뜁니다.")

    # 데이터 누락 분석
    only_in_raw = merged_df[merged_df['_merge'] == 'left_only']
    only_in_db = merged_df[merged_df['_merge'] == 'right_only']

    print(f"\n데이터 누락 분석:")
    print(f"원본에만 있는 레코드 수: {len(only_in_raw)}")
    print(f"DB에만 있는 레코드 수: {len(only_in_db)}")

    if len(only_in_raw) > 0:
        print("\n원본에만 있는 레코드 샘플 (최대 5개):")
        print(only_in_raw[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head())

    if len(only_in_db) > 0:
        print("\nDB에만 있는 레코드 샘플 (최대 5개):")
        print(only_in_db[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head())

def main():
    raw_df = get_raw_data()
    db_df = get_db_data()
    compare_data(raw_df, db_df)

if __name__ == "__main__":
    main()