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

# OrderType 테이블 생성
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS OrderType (
    EDI_Order_Type VARCHAR(255) PRIMARY KEY,
    Detailed_Order_Type VARCHAR(255)
)
"""
)

# Receiving_TAT_Report 테이블 생성
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS Receiving_TAT_Report (
    ReceiptNo VARCHAR(255) PRIMARY KEY,
    Customer_Order_No VARCHAR(255),
    PO_No VARCHAR(255),
    Part VARCHAR(255),
    EDI_Order_Type VARCHAR(255),
    Ship_From VARCHAR(255),
    Ship_to VARCHAR(255),
    Country VARCHAR(255),
    Quantity BIGINT(45),
    PutAwayDate DATETIME,
    Dell_FY VARCHAR(20),
    Quarter VARCHAR(10),
    Month VARCHAR(20),
    Dell_Week VARCHAR(10),
    OrderType VARCHAR(255),
    Count_RC INT,
    Count_PO INT,
    FOREIGN KEY (EDI_Order_Type) REFERENCES OrderType(EDI_Order_Type)
)
"""
)

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
xlsx_complete_folder = os.path.join("C:\\MyMain\\Teckwah\\download\\", "xlsx_files_complete")

# 폴더가 존재하지 않으면 생성
os.makedirs(xlsx_complete_folder, exist_ok=True)

def get_latest_file(download_folder):
    """최신 다운로드된 파일을 찾습니다."""
    files = [os.path.join(download_folder, f) for f in os.listdir(download_folder) if f.endswith(".xlsx")]
    latest_file = max(files, key=os.path.getctime)
    return latest_file

def get_dell_week_and_fy(date_str):
    """날짜를 Dell-Week와 Dell_FY로 변환합니다."""
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    fy_start_month = 2
    fy_start_day = 1

    if date.month < fy_start_month or (date.month == fy_start_month and date.day < fy_start_day):
        fy = date.year - 1
    else:
        fy = date.year

    fy_start_date = datetime.datetime(fy, fy_start_month, fy_start_day)
    delta = date - fy_start_date
    dell_week = (delta.days // 7) + 1
    dell_week_str = f"WK{str(dell_week).zfill(2)}"
    fy_str = f"FY{str(fy % 100).zfill(2)}"

    return dell_week_str, fy_str

def get_quarter(date_str):
    """날짜를 분기로 변환합니다."""
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return f"Q{((date.month - 1) // 3) + 1}"

def get_order_type(edi_order_type):
    """EDI_Order_Type을 OrderType으로 변환합니다."""
    order_type_mapping = {
        "BALANCE-IN": "P3",
        "REPLEN-IN": "P3",
        "PNAE-IN": "P1",
        "PNAC-IN": "P1",
        "DISPOSE-IN": "P6",
        "PURGE-IN": "P6",
    }
    return order_type_mapping.get(edi_order_type, "Unknown")

def rename_columns(df):
    """컬럼명을 일치시킵니다."""
    rename_columns = {
        "ReceiptNo": "ReceiptNo",
        "Replen/Balance Order#": "Customer_Order_No",
        "Cust Sys No": "PO_No",
        "Allocated Part#": "Part",
        "EDI Order Type": "EDI_Order_Type",
        "ShipFromCode": "Ship_From",
        "ShipToCode": "Ship_to",
        "Country": "Country",
        "Quantity": "Quantity",
        "PutAwayDate": "PutAwayDate",
    }
    return df.rename(columns=rename_columns)

def upload_to_mysql(xlsx_file, file_path):
    """엑셀 파일 데이터를 MySQL에 업로드합니다."""
    df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")

    expected_columns = [
        "ReceiptNo",
        "Replen/Balance Order#",
        "Cust Sys No",
        "Allocated Part#",
        "EDI Order Type",
        "ShipFromCode",
        "ShipToCode",
        "Country",
        "Quantity",
        "PutAwayDate",
    ]

    available_columns = [col for col in expected_columns if col in df.columns]
    df = df[available_columns]
    df = rename_columns(df)

    # Country 컬럼에서 'KR' 값만 필터링
    df = df[df['Country'] == 'KR']

    df = df.where(pd.notnull(df), None)
    df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], format="%m/%d/%Y %H:%M:%S", errors="coerce")
    df["PutAwayDate"] = df["PutAwayDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Quantity를 숫자가 아닌 값을 0으로 대체
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors='coerce').fillna(0)

    df["Dell_Week"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[0] if pd.notnull(x) else None)
    df["Dell_FY"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[1] if pd.notnull(x) else None)
    df["Quarter"] = df["PutAwayDate"].apply(lambda x: get_quarter(x) if pd.notnull(x) else None)
    df["OrderType"] = df["EDI_Order_Type"].apply(get_order_type)
    df["Month"] = df["PutAwayDate"].apply(lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%B") if pd.notnull(x) else None))

    df["Count_RC"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])["ReceiptNo"].transform("count")
    df["Count_PO"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])["Customer_Order_No"].transform("count")

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    order_types = df[["EDI_Order_Type", "OrderType"]].drop_duplicates()
    for index, row in order_types.iterrows():
        cursor.execute(
            """
            INSERT INTO OrderType (EDI_Order_Type, Detailed_Order_Type)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Detailed_Order_Type=VALUES(Detailed_Order_Type)
            """,
            tuple(row),
        )

    insert_columns = [
        "ReceiptNo",
        "Customer_Order_No",
        "PO_No",
        "Part",
        "EDI_Order_Type",
        "Ship_From",
        "Ship_to",
        "Country",
        "Quantity",
        "PutAwayDate",
        "Dell_FY",
        "Quarter",
        "Month",
        "Dell_Week",
        "OrderType",
        "Count_RC",
        "Count_PO"
    ]
    insert_placeholders = ", ".join(["%s"] * len(insert_columns))

    for index, row in df.iterrows():
        cursor.execute(
            f"""
            INSERT INTO Receiving_TAT_Report ({", ".join(insert_columns)}) 
            VALUES ({insert_placeholders})
            ON DUPLICATE KEY UPDATE 
                Customer_Order_No=VALUES(Customer_Order_No),
                PO_No=VALUES(PO_No),
                Part=VALUES(Part),
                EDI_Order_Type=VALUES(EDI_Order_Type),
                Ship_From=VALUES(Ship_From),
                Ship_to=VALUES(Ship_to),
                Country=VALUES(Country),
                Quantity=VALUES(Quantity),
                PutAwayDate=VALUES(PutAwayDate),
                Dell_Week=VALUES(Dell_Week),
                Dell_FY=VALUES(Dell_FY),
                Quarter=VALUES(Quarter),
                OrderType=VALUES(OrderType),
                Month=VALUES(Month),
                Count_RC=VALUES(Count_RC),
                Count_PO=VALUES(Count_PO)
            """,
            tuple(None if pd.isnull(x) else x for x in row),
        )

    conn.commit()
    cursor.close()
    conn.close()

    shutil.move(file_path, os.path.join(xlsx_complete_folder, xlsx_file))
    print(f"파일 처리 완료: {xlsx_file}")

if __name__ == "__main__":
    xlsx_file_name = "3.012_CS_Receiving_TAT_Report.xlsx"

    if os.path.exists(os.path.join(download_folder, xlsx_file_name)):
        latest_file = get_latest_file(download_folder)
        upload_to_mysql(xlsx_file_name, latest_file)
