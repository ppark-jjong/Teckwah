import pandas as pd
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
cursor.execute(
    """
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
    OrderType VARCHAR(255)
)
"""
)

conn.commit()
cursor.close()
conn.close()

# 엑셀 파일 경로 설정
xlsx_file_path = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete\\3.012_CS_Receiving_TAT_Report.xlsx"

# 엑셀 데이터 읽기
df_excel = pd.read_excel(xlsx_file_path, sheet_name="CS Receiving TAT")

# 필요한 컬럼만 선택하고 컬럼명 일치시키기
df_excel = df_excel[
    [
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
]

# 컬럼명 일치시키기
df_excel.columns = [
    "ReceiptNo",
    "Replen_Balance_Order_No",
    "Cust_Sys_No",
    "Allocated_Part_No",
    "EDI_Order_Type",
    "Ship_From_Code",
    "Ship_To_Code",
    "Country",
    "Quantity",
    "PutAwayDate",
]

# 데이터 유형 변환
df_excel["Replen_Balance_Order_No"] = df_excel["Replen_Balance_Order_No"].astype(str)
df_excel["PutAwayDate"] = pd.to_datetime(
    df_excel["PutAwayDate"], format="%m/%d/%Y %H:%M:%S"
)
df_excel["PutAwayDate"] = df_excel["PutAwayDate"].dt.strftime("%Y-%m-%d %H:%M:%S")


# Date 값을 Dell-Week와 FY로 변환하는 함수
def get_dell_week_and_fy(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    fy_start_month = 2
    fy_start_day = 1

    if date.month < fy_start_month or (
        date.month == fy_start_month and date.day < fy_start_day
    ):
        fy = date.year - 1
    else:
        fy = date.year

    fy_start_date = datetime.datetime(fy, fy_start_month, fy_start_day)
    delta = date - fy_start_date
    dell_week = (delta.days // 7) + 1
    dell_week_str = f"WK{str(dell_week).zfill(2)}"
    fy_str = f"FY{str(fy % 100).zfill(2)}"

    return dell_week_str, fy_str


# Quarter 계산 함수
def get_quarter(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return f"Q{((date.month - 1) // 3) + 1}"


# OrderType 계산 함수
def get_order_type(edi_order_type):
    # EDI_Order_Type을 기반으로 OrderType을 결정하는 로직이 이미 존재한다고 가정
    # 필요시 구체적인 매핑 로직 추가
    return edi_order_type


# 추가 컬럼 계산
df_excel["Dell_Week"] = df_excel["PutAwayDate"].apply(
    lambda x: get_dell_week_and_fy(x)[0]
)
df_excel["FY"] = df_excel["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x)[1])
df_excel["Quarter"] = df_excel["PutAwayDate"].apply(get_quarter)
df_excel["OrderType"] = df_excel["EDI_Order_Type"].apply(get_order_type)


# 데이터베이스에 데이터 삽입
def insert_data_into_db(df):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            """
        INSERT INTO Receiving_TAT_Report (ReceiptNo, Replen_Balance_Order_No, Cust_Sys_No,
                                          Allocated_Part_No, EDI_Order_Type, Ship_From_Code,
                                          Ship_To_Code, Country, Quantity, PutAwayDate,
                                          Dell_Week, FY, Quarter, OrderType)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            OrderType=VALUES(OrderType)
        """,
            (
                row["ReceiptNo"],
                row["Replen_Balance_Order_No"],
                row["Cust_Sys_No"],
                row["Allocated_Part_No"],
                row["EDI_Order_Type"],
                row["Ship_From_Code"],
                row["Ship_To_Code"],
                row["Country"],
                row["Quantity"],
                row["PutAwayDate"],
                row["Dell_Week"],
                row["FY"],
                row["Quarter"],
                row["OrderType"],
            ),
        )
    conn.commit()
    cursor.close()
    conn.close()


# 데이터 삽입
insert_data_into_db(df_excel)


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
        PutAwayDate,
        Dell_Week,
        FY,
        Quarter,
        OrderType
    FROM Receiving_TAT_Report
    """
    df_db = pd.read_sql(query, conn)
    conn.close()
    return df_db


# 데이터 비교
def compare_dataframes(df1, df2):
    # 필요한 컬럼만 선택
    columns_to_compare = [
        "ReceiptNo",
        "Replen_Balance_Order_No",
        "Cust_Sys_No",
        "Allocated_Part_No",
        "EDI_Order_Type",
        "Ship_From_Code",
        "Ship_To_Code",
        "Country",
        "Quantity",
        "PutAwayDate",
        "Dell_Week",
        "FY",
        "Quarter",
        "OrderType",
    ]

    df1 = df1[columns_to_compare]
    df2 = df2[columns_to_compare]

    # 데이터 유형 맞추기
    df1["Replen_Balance_Order_No"] = df1["Replen_Balance_Order_No"].astype(str)
    df2["Replen_Balance_Order_No"] = df2["Replen_Balance_Order_No"].astype(str)

    df1["PutAwayDate"] = pd.to_datetime(df1["PutAwayDate"])
    df2["PutAwayDate"] = pd.to_datetime(df2["PutAwayDate"])

    # 비교
    comparison = df1.merge(df2, indicator=True, how="outer")
    difference = comparison[comparison["_merge"] != "both"]
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
