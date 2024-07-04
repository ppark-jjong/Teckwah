import os
import pandas as pd
import shutil
import mysql.connector
from mysql.connector import pooling
from datetime import datetime, timedelta
from typing import Tuple, Dict

# 설정
DB_CONFIG = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,
}
DOWNLOAD_FOLDER = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
COMPLETE_FOLDER = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete"

# 폴더 생성
os.makedirs(COMPLETE_FOLDER, exist_ok=True)

# 데이터베이스 연결 풀 생성
connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **DB_CONFIG)

def get_db_connection():
    return connection_pool.get_connection()

def execute_query(query: str, params: tuple = None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
        conn.commit()

def create_tables():
    order_type_table = """
    CREATE TABLE IF NOT EXISTS OrderType (
        EDI_Order_Type VARCHAR(255) PRIMARY KEY,
        Detailed_Order_Type VARCHAR(255)
    )
    """
    receiving_tat_table = """
    CREATE TABLE IF NOT EXISTS Receiving_TAT_Report (
        ReceiptNo VARCHAR(255) PRIMARY KEY,
        Customer_Order_No VARCHAR(255),
        PO_No VARCHAR(255),
        Part VARCHAR(255),
        EDI_Order_Type VARCHAR(255),
        Ship_From VARCHAR(255),
        Ship_to VARCHAR(255),
        Country VARCHAR(255),
        Quantity BIGINT,
        PutAwayDate DATETIME,
        Dell_FY VARCHAR(20),
        Quarter VARCHAR(10),
        Month VARCHAR(2),
        Dell_Week VARCHAR(10),
        OrderType VARCHAR(255),
        Count_RC INT,
        Count_PO INT,
        FOREIGN KEY (EDI_Order_Type) REFERENCES OrderType(EDI_Order_Type)
    )
    """
    execute_query(order_type_table)
    execute_query(receiving_tat_table)

def get_latest_file(folder: str) -> str:
    return max(
        [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".xlsx")],
        key=os.path.getctime
    )

def get_fy_start(year: int) -> datetime:
    first_day = datetime(year, 2, 1)
    return first_day + timedelta(days=(5 - first_day.weekday() + 7) % 7)

def get_dell_week_and_fy(date: datetime) -> Tuple[str, str]:
    fy_start = get_fy_start(date.year if date >= get_fy_start(date.year) else date.year - 1)
    fy = date.year + 1 if date >= fy_start else date.year
    dell_week = min((date - fy_start).days // 7 + 1, 52)
    return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"

def get_quarter(date: datetime) -> str:
    _, fy = get_dell_week_and_fy(date)
    fy_year = int(fy[2:]) + 2000
    fy_start = get_fy_start(fy_year - 1)
    quarter = min((date - fy_start).days // 91 + 1, 4)
    return f"Q{quarter}"

ORDER_TYPE_MAPPING = {
    "BALANCE-IN": "P3",
    "REPLEN-IN": "P3",
    "PNAE-IN": "P1",
    "PNAC-IN": "P1",
    "DISPOSE-IN": "P6",
    "PURGE-IN": "Purge",
}

def get_order_type(edi_order_type: str) -> str:
    return ORDER_TYPE_MAPPING.get(edi_order_type, "Unknown")

COLUMN_MAPPING = {
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

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)
    df = df[df["Country"] == "KR"].copy()
    df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], format="%m/%d/%Y %H:%M:%S", errors="coerce")
    
    df["Dell_FY"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[1] if pd.notnull(x) else None)
    df["Quarter"] = df["PutAwayDate"].apply(lambda x: get_quarter(x) if pd.notnull(x) else None)
    df["Month"] = df["PutAwayDate"].dt.strftime("%m")
    df["Dell_Week"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[0] if pd.notnull(x) else None)
    df["OrderType"] = df["EDI_Order_Type"].map(ORDER_TYPE_MAPPING)

    df["Count_RC"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])["ReceiptNo"].transform("count")
    df["Count_PO"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])["Customer_Order_No"].transform("count")

    return df

def upload_to_mysql(df: pd.DataFrame):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Insert OrderType data
            for edi_type, detailed_type in ORDER_TYPE_MAPPING.items():
                cursor.execute(
                    """
                    INSERT INTO OrderType (EDI_Order_Type, Detailed_Order_Type)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE Detailed_Order_Type=VALUES(Detailed_Order_Type)
                    """,
                    (edi_type, detailed_type)
                )

            # Insert Receiving_TAT_Report data
            insert_columns = [
                "ReceiptNo", "Customer_Order_No", "PO_No", "Part", "EDI_Order_Type",
                "Ship_From", "Ship_to", "Country", "Quantity", "PutAwayDate",
                "Dell_FY", "Quarter", "Month", "Dell_Week", "OrderType", "Count_RC", "Count_PO"
            ]
            insert_placeholders = ", ".join(["%s"] * len(insert_columns))
            update_placeholders = ", ".join([f"{col}=VALUES({col})" for col in insert_columns[1:]])

            insert_query = f"""
                INSERT INTO Receiving_TAT_Report ({", ".join(insert_columns)}) 
                VALUES ({insert_placeholders})
                ON DUPLICATE KEY UPDATE {update_placeholders}
            """

            cursor.executemany(
                insert_query,
                df[insert_columns].where(pd.notnull(df), None).values.tolist()
            )

        conn.commit()

def process_file(file_path: str):
    try:
        df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")
        df = process_dataframe(df)
        upload_to_mysql(df)
        shutil.move(file_path, os.path.join(COMPLETE_FOLDER, os.path.basename(file_path)))
        print(f"파일 처리 완료: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

if __name__ == "__main__":
    create_tables()
    xlsx_file_name = "3.012_CS_Receiving_TAT_Report.xlsx"
    file_path = os.path.join(DOWNLOAD_FOLDER, xlsx_file_name)
    
    if os.path.exists(file_path):
        process_file(file_path)
    else:
        print(f"File not found: {xlsx_file_name}")