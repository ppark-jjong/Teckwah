import pandas as pd
import mysql.connector
import datetime

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
}

# 엑셀 파일 경로 설정
excel_file_path = "C:\\MyMain\\Teckwah\\download\\xlsx_files\\3.012_CS_Receiving_TAT_Report.xlsx"

def get_database_data(query):
    """데이터베이스에서 데이터를 조회합니다."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

def get_excel_data(file_path):
    """엑셀 파일에서 데이터를 읽어옵니다."""
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
    df = df[df["Country"] == "KR"]
    df["PutAwayDate"] = pd.to_datetime(
        df["PutAwayDate"], format="%m/%d/%Y %H:%M:%S", errors="coerce"
    )
    df["PutAwayDate"] = df["PutAwayDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df["Dell_FY"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[1] if pd.notnull(x) else None)
    df["Quarter"] = df["PutAwayDate"].apply(lambda x: get_quarter(x) if pd.notnull(x) else None)
    df["Month"] = df["PutAwayDate"].apply(lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%m") if pd.notnull(x) else None))
    df["Dell_Week"] = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[0] if pd.notnull(x) else None)
    df["OrderType"] = df["EDI_Order_Type"].apply(get_order_type)

    return df

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

def get_fy_start(year):
    first_day = datetime.datetime(year, 2, 1)
    days_until_saturday = (5 - first_day.weekday() + 7) % 7
    return first_day + datetime.timedelta(days=days_until_saturday)

def get_dell_week_and_fy(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

    fy_start = get_fy_start(date.year)
    if date < fy_start:
        fy_start = get_fy_start(date.year - 1)
        fy = date.year - 1
    else:
        fy = date.year

    days_since_fy_start = (date - fy_start).days
    dell_week = (days_since_fy_start // 7) + 1

    if dell_week > 52:
        dell_week = 52

    dell_week_str = f"WK{str(dell_week).zfill(2)}"
    fy_str = f"FY{str(fy % 100).zfill(2)}"

    return dell_week_str, fy_str

def get_quarter(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    _, fy = get_dell_week_and_fy(date_str)
    fy_year = int(fy[2:]) + 2000
    fy_start = get_fy_start(fy_year - 1)

    days_since_fy_start = (date - fy_start).days
    quarter = (days_since_fy_start // 91) + 1
    if quarter > 4:
        quarter = 4

    return f"Q{quarter}"

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

def compare_data(df, db_data):
    """엑셀 데이터와 데이터베이스 데이터를 비교합니다."""
    db_df = pd.DataFrame(db_data)
    comparison = df.merge(db_df, on="ReceiptNo", suffixes=('_excel', '_db'), how='outer', indicator=True)
    diff = comparison[comparison['_merge'] != 'both']
    return diff

if __name__ == "__main__":
    excel_data = get_excel_data(excel_file_path)
    query = "SELECT * FROM Receiving_TAT_Report"
    db_data = get_database_data(query)
    
    differences = compare_data(excel_data, db_data)
    if differences.empty:
        print("엑셀 파일과 데이터베이스 데이터가 일치합니다.")
    else:
        print("엑셀 파일과 데이터베이스 데이터가 일치하지 않습니다. 차이점은 다음과 같습니다:")
        print(differences)
