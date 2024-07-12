from typing import Dict, Any

# 데이터베이스 설정
DB_CONFIG: Dict[str, Any] = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,
}

# 폴더 경로 설정
DOWNLOAD_FOLDER: str = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
COMPLETE_FOLDER: str = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete"

# 연결 풀 설정
POOL_NAME: str = "mypool"
POOL_SIZE: int = 5

# 타임아웃 설정
DOWNLOAD_TIMEOUT: int = 120  # 초
WEBDRIVER_TIMEOUT: int = 30  # 초

# 주문 유형 매핑
ORDER_TYPE_MAPPING: Dict[str, str] = {
    "BALANCE-IN": "P3",
    "REPLEN-IN": "P3",
    "PNAE-IN": "P1",
    "PNAC-IN": "P1",
    "DISPOSE-IN": "P6",
    "PURGE-IN": "Purge",
}

# 열 매핑
COLUMN_MAPPING: Dict[str, str] = {
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
ORDER_TYPE_TABLE: str = "OrderType"
RECEIVING_TAT_REPORT_TABLE: str = "Receiving_TAT_Report"

# 웹 크롤링 설정
MAX_RETRIES: int = 3
RETRY_DELAY: int = 5  # 초
