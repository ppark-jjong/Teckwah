import os
import pandas as pd
import openpyxl
import shutil
import mysql.connector

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,  # LOCAL INFILE 옵션 활성화
}

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

# 데이터베이스 연결 및 데이터 삽입
def upload_to_mysql(xlsx_file, file_path):
    xlsx_path = os.path.join(download_folder, xlsx_file)

    
    # 엑셀 파일 읽기
    df = pd.read_excel(file_path, sheet_name='CS Receiving TAT')
    
    # 필요한 컬럼만 선택하고 컬럼명 일치시키기
    df = df[['ReceiptNo', 'Replen/Balance Order#', 'Cust Sys No', 
             'Allocated Part#', 'EDI Order Type', 'ShipFromCode', 
             'ShipToCode', 'Country', 'Quantity', 'PutAwayDate']]

    # 컬럼명 일치시키기
    df.columns = [
        'ReceiptNo', 'Replen_Balance_Order_No', 'Cust_Sys_No', 
        'Allocated_Part_No', 'EDI_Order_Type', 'Ship_From_Code', 
        'Ship_To_Code', 'Country', 'Quantity', 'PutAwayDate'
    ]

    # PutAwayDate 컬럼 형식 변환
    df['PutAwayDate'] = pd.to_datetime(df['PutAwayDate'], format='%m/%d/%Y %H:%M:%S')
    df['PutAwayDate'] = df['PutAwayDate'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # MySQL 연결
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # 데이터 삽입
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Receiving_TAT_Report (ReceiptNo, Replen_Balance_Order_No, Cust_Sys_No, 
                                               Allocated_Part_No, EDI_Order_Type, Ship_From_Code, 
                                               Ship_To_Code, Country, Quantity, PutAwayDate) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

   # 파일을 완료 폴더로 이동
    shutil.move(xlsx_path, os.path.join(xlsx_complete_folder, xlsx_file))
    print(f"파일 처리 완료: {xlsx_file}")
    # 커밋 및 연결 종료
    conn.commit()
    cursor.close()
    conn.close()

# 메인 실행 부분
if __name__ == "__main__":
    xlsx_file_name = "3.012_CS_Receiving_TAT_Report.xlsx"
    if os.path.exists(os.path.join(download_folder, xlsx_file_name)):
        latest_file = get_latest_file(download_folder)
        upload_to_mysql(xlsx_file_name, latest_file)    
    else:
        print(f"파일이 존재하지 않습니다: {xlsx_file_name}")
