import os

# 데이터베이스 설정
DB_CONFIG = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,
}

# 다운로드 폴더 경로 설정
DOWNLOAD_FOLDER = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
COMPLETE_FOLDER = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete"

# 연결 풀 설정
POOL_NAME = "mypool"
POOL_SIZE = 5

# 타임아웃 설정
DOWNLOAD_TIMEOUT = 120  # 초

# 주문 유형 매핑
ORDER_TYPE_MAPPING = {
    "BALANCE-IN": "P3",
    "REPLEN-IN": "P3",
    "PNAE-IN": "P1",
    "PNAC-IN": "P1",
    "DISPOSE-IN": "P6",
    "PURGE-IN": "Purge",
}

# 열 매핑
COLUMN_MAPPING = {
    "ReceiptNo": "ReceiptNo",
    "Replen/Balance Order#": "Replen_Balance_Order",
    "Cust Sys No": "Cust_Sys_No",
    "Allocated Part#": "Allocated_Part",
    "EDI Order Type": "EDI_Order_Type",
    "ShipFromCode": "ShipFromCode",
    "ShipToCode": "ShipToCode",
    "Country": "Country",
    "Quantity": "Quantity",
    "PutAwayDate": "PutAwayDate",
}

# 테이블 이름
ORDER_TYPE_TABLE = "OrderType"
RECEIVING_TAT_REPORT_TABLE = "Receiving_TAT_Report"

# 폴더 생성
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(COMPLETE_FOLDER, exist_ok=True)