import os
import pandas as pd
import openpyxl
import shutil
import mysql.connector
import datetime

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,
}

# MySQL 데이터베이스 연결
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# 테이블 생성
cursor.execute('''
CREATE TABLE IF NOT EXISTS OrderType (
    EDI_Order_Type VARCHAR(255) PRIMARY KEY,
    Detailed_Order_Type VARCHAR(255)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Receiving_TAT_Report (
    ReceiptNo VARCHAR(255) PRIMARY KEY,
    Replen_Balance_Order_No VARCHAR(255),
    Cust_Sys_No VARCHAR(255),
    Allocated_Part_No VARCHAR(255),
    EDI_Order_Type VARCHAR(255),
    Ship_From_Code VARCHAR(255),
    Ship_To_Code VARCHAR(255),
    Country VARCHAR(255),
    Quantity INT,
    PutAwayDate DATETIME,
    Dell_Week VARCHAR(10),
    FY VARCHAR(10),
    Quarter VARCHAR(10),
    OrderType VARCHAR(255),
    Count_RC INT,
    Count_PO INT,
    FOREIGN KEY (EDI_Order_Type) REFERENCES OrderType(EDI_Order_Type)
)
''')

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
xlsx_complete_folder = os.path.join("C:\\MyMain\\Teckwah\\download\\", "xlsx_files_complete")

# 폴더가 존재하지 않으면 생성
os.makedirs(xlsx_complete_folder, exist_ok=True)

# 최신 다운로드된 파일 찾기
def get_latest_file(download_folder):
    files = [os.path.join(download_folder, f) for f in os.listdir(download_folder) if f.endswith('.xlsx')]
    latest_file = max(files, key=os.path.getctime)
    return latest_file

# Date 값을 Dell-Week와 FY로 변환하는 함수
def get_dell_week_and_fy(date_str):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    fy_start_month = 2
    fy_start_day = 1

    if date.month < fy_start_month or (date.month == fy_start_month and date.day < fy_start_day):
        fy = date.year - 1
    else:
        fy = date.year

    fy_start_date = datetime.datetime(fy, fy_start_month, fy_start_day)
    delta = date - fy_start_date
    dell_week = (delta.days // 7) + 1
    dell_week_str = f'WK{str(dell_week).zfill(2)}'
    fy_str = f'FY{str(fy % 100).zfill(2)}'

    return dell_week_str, fy_str

# Quarter 계산 함수
def get_quarter(date_str):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return f'Q{((date.month - 1) // 3) + 1}'

# OrderType 계산 함수
def get_order_type(edi_order_type):
    # EDI_Order_Type을 기반으로 OrderType을 결정하는 매핑 로직 추가
    order_type_mapping = {
        'BALANCE-IN': 'P3 - Normal',
        'REPLEN-IN': 'P3 - Normal',
        'PNAE-IN': 'P1 - PNA',
        'PNAC-IN': 'P1 - PNA',
        'DISPOSE-IN': 'P6 - E&O'
        # 추가적인 매핑이 필요하면 여기서 추가
    }
    return order_type_mapping.get(edi_order_type, 'Unknown')

# 데이터베이스 연결 및 데이터 삽입
def upload_to_mysql(xlsx_file, file_path):
    # 엑셀 파일 읽기
    df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")
    
    # 필요한 컬럼만 선택하고 컬럼명 일치시키기
    expected_columns = [
        'ReceiptNo', 'Replen/Balance Order#', 'Cust Sys No', 
        'Allocated Part#', 'EDI Order Type', 'ShipFromCode', 
        'ShipToCode', 'Country', 'Quantity', 'PutAwayDate'
    ]
    
    available_columns = [col for col in expected_columns if col in df.columns]
    
    df = df[available_columns]

    # 컬럼명 일치시키기
    rename_columns = {
        'ReceiptNo': 'ReceiptNo', 
        'Replen/Balance Order#': 'Replen_Balance_Order_No', 
        'Cust Sys No': 'Cust_Sys_No', 
        'Allocated Part#': 'Allocated_Part_No', 
        'EDI Order Type': 'EDI_Order_Type', 
        'ShipFromCode': 'Ship_From_Code', 
        'ShipToCode': 'Ship_To_Code', 
        'Country': 'Country', 
        'Quantity': 'Quantity', 
        'PutAwayDate': 'PutAwayDate'
    }
    
    df = df.rename(columns=rename_columns)

    # PutAwayDate 컬럼 형식 변환
    df['PutAwayDate'] = pd.to_datetime(df['PutAwayDate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['PutAwayDate'] = df['PutAwayDate'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 추가 컬럼 계산
    df['Dell_Week'] = df['PutAwayDate'].apply(lambda x: get_dell_week_and_fy(x)[0] if pd.notnull(x) else None)
    df['FY'] = df['PutAwayDate'].apply(lambda x: get_dell_week_and_fy(x)[1] if pd.notnull(x) else None)
    df['Quarter'] = df['PutAwayDate'].apply(lambda x: get_quarter(x) if pd.notnull(x) else None)
    df['OrderType'] = df['EDI_Order_Type'].apply(get_order_type)

    # Count RC와 Count PO 값 계산
    df['Count_RC'] = df.groupby(['ReceiptNo', 'EDI_Order_Type'])['ReceiptNo'].transform('count')
    df['Count_PO'] = df.groupby(['ReceiptNo', 'EDI_Order_Type'])['Replen_Balance_Order_No'].transform('count')

    # MySQL 연결
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # OrderType 테이블에 데이터 삽입
    order_types = df[['EDI_Order_Type', 'OrderType']].drop_duplicates()
    for index, row in order_types.iterrows():
        cursor.execute("""
            INSERT INTO OrderType (EDI_Order_Type, Detailed_Order_Type)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Detailed_Order_Type=VALUES(Detailed_Order_Type)
        """, tuple(row))

    # 데이터 삽입
    insert_columns = df.columns.tolist()
    insert_placeholders = ", ".join(["%s"] * len(insert_columns))
    
    for index, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO Receiving_TAT_Report ({", ".join(insert_columns)}) 
            VALUES ({insert_placeholders})
            ON DUPLICATE KEY UPDATE 
                Replen_Balance_Order_No=VALUES(Replen_Balance_Order_No),
                Cust_Sys_No=VALUES(Cust_Sys_No),
                Allocated_Part_No=VALUES(Allocated_Part_No),
                EDI_Order_Type=VALUES(EDI_Order_Type),
                Ship_From_Code=VALUES(Ship_From_Code),
                Ship_To_Code=VALUES(Ship_To_Code),
                Country=VALUES(Country),
                Quantity=VALUES(Quantity),
                PutAwayDate=VALUES(PutAwayDate),
                Dell_Week=VALUES(Dell_Week),
                FY=VALUES(FY),
                Quarter=VALUES(Quarter),
                OrderType=VALUES(OrderType),
                Count_RC=VALUES(Count_RC),
                Count_PO=VALUES(Count_PO)
            """, tuple(row))

    # 커밋 및 연결 종료
    conn.commit()
    cursor.close()
    conn.close()

    # 파일을 완료 폴더로 이동
    shutil.move(file_path, os.path.join(xlsx_complete_folder, xlsx_file))
    print(f"파일 처리 완료: {xlsx_file}")   

# 메인 실행 부분
if __name__ == "__main__":
    xlsx_file_name = "3.012_CS_Receiving_TAT_Report.xlsx"
    if os.path.exists(os.path.join(download_folder, xlsx_file_name)):
        latest_file = get_latest_file(download_folder)
        upload_to_mysql(xlsx_file_name, latest_file)
    else:
        print(f"파일이 존재하지 않습니다: {xlsx_file_name}")
