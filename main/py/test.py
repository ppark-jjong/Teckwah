import pandas as pd
import mysql.connector

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,  # LOCAL INFILE 옵션 활성화
}

# 엑셀 파일 경로 설정
xlsx_file_path = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete\\3.012_CS_Receiving_TAT_Report.xlsx"

# 엑셀 데이터 읽기
df_excel = pd.read_excel(xlsx_file_path, sheet_name='CS Receiving TAT')

# 필요한 컬럼만 선택하고 컬럼명 일치시키기
df_excel = df_excel[['ReceiptNo', 'Replen/Balance Order#', 'Cust Sys No',
                     'Allocated Part#', 'EDI Order Type', 'ShipFromCode',
                     'ShipToCode', 'Country', 'Quantity', 'PutAwayDate']]

# 컬럼명 일치시키기
df_excel.columns = [
    'ReceiptNo', 'Replen_Balance_Order_No', 'Cust_Sys_No',
    'Allocated_Part_No', 'EDI_Order_Type', 'Ship_From_Code',
    'Ship_To_Code', 'Country', 'Quantity', 'PutAwayDate'
]

# 데이터 유형 변환
df_excel['Replen_Balance_Order_No'] = df_excel['Replen_Balance_Order_No'].astype(str)
df_excel['PutAwayDate'] = pd.to_datetime(df_excel['PutAwayDate'], format='%m/%d/%Y %H:%M:%S')
df_excel['PutAwayDate'] = df_excel['PutAwayDate'].dt.strftime('%Y-%m-%d %H:%M:%S')

# 데이터베이스에서 데이터 가져오기
def fetch_from_mysql():
    conn = mysql.connector.connect(**db_config)
    query = """
    SELECT 
        ReceiptNo, 
        Replen_Balance_Order_No, 
        Cust_Sys_No, 
        Allocated_Part_No, 
        EDI_Order_Type, 
        Ship_From_Code, 
        Ship_To_Code, 
        Country, 
        Quantity, 
        PutAwayDate 
    FROM Receiving_TAT_Report
    """
    df_db = pd.read_sql(query, conn)
    conn.close()
    return df_db

# 데이터 비교
def compare_dataframes(df1, df2):
    # 데이터 유형 맞추기
    df1['Replen_Balance_Order_No'] = df1['Replen_Balance_Order_No'].astype(str)
    df2['Replen_Balance_Order_No'] = df2['Replen_Balance_Order_No'].astype(str)
    
    df1['PutAwayDate'] = pd.to_datetime(df1['PutAwayDate'])
    df2['PutAwayDate'] = pd.to_datetime(df2['PutAwayDate'])
    
    # 비교
    comparison = df1.merge(df2, indicator=True, how='outer')
    difference = comparison[comparison['_merge'] != 'both']
    return difference

# 데이터 비교 수행
df_db = fetch_from_mysql()
differences = compare_dataframes(df_excel, df_db)

# 비교 결과 출력
if differences.empty:
    print("엑셀 데이터와 데이터베이스 데이터가 정확히 일치합니다.")
else:
    print("엑셀 데이터와 데이터베이스 데이터가 일치하지 않습니다.")
    print(differences)
