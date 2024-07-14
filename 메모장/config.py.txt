from typing import Dict, Any

DB_CONFIG: Dict[str, Any] = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,
}

DOWNLOAD_FOLDER: str = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
COMPLETE_FOLDER: str = "C:\\MyMain\\Teckwah\\download\\xlsx_files_complete"

POOL_NAME: str = "mypool"
POOL_SIZE: int = 5

DOWNLOAD_TIMEOUT: int = 120
WEBDRIVER_TIMEOUT: int = 30

ORDER_TYPE_MAPPING: Dict[str, str] = {
    "BALANCE-IN": "P3",
    "REPLEN-IN": "P3",
    "PNAE-IN": "P1",
    "PNAC-IN": "P1",
    "DISPOSE-IN": "P6",
    "PURGE-IN": "Purge",
}

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

ORDER_TYPE_TABLE: str = "OrderType"
RECEIVING_TAT_REPORT_TABLE: str = "Receiving_TAT_Report"

MAX_RETRIES: int = 3
RETRY_DELAY: int = 5