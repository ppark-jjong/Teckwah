import mysql.connector
from mysql.connector import pooling
from mysql.connector import Error
import pandas as pd
from config import DB_CONFIG, POOL_NAME, POOL_SIZE, ORDER_TYPE_TABLE, RECEIVING_TAT_REPORT_TABLE, ORDER_TYPE_MAPPING
class MySQLConnectionPool:
    def __init__(self):
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=POOL_NAME,
            pool_size=POOL_SIZE,
            **DB_CONFIG
        )

    def __enter__(self):
        self.connection = self.pool.get_connection()
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except Error as e:
            print(f"쿼리 실행 실패: {e}")
            self.connection.rollback()

    def executemany(self, query, params):
        try:
            self.cursor.executemany(query, params)
            self.connection.commit()
        except Error as e:
            print(f"대량 쿼리 실행 실패: {e}")
            self.connection.rollback()

def upload_to_mysql(df):
    with MySQLConnectionPool() as conn:
        # Insert OrderType data
        for edi_type, detailed_type in ORDER_TYPE_MAPPING.items():
            conn.execute_query(
                f"""
                INSERT INTO {ORDER_TYPE_TABLE} (EDI_Order_Type, Detailed_Order_Type)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Detailed_Order_Type=VALUES(Detailed_Order_Type)
                """,
                (edi_type, detailed_type)
            )

        # Insert Receiving_TAT_Report data
        insert_columns = [
            "ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No", "Allocated_Part", "EDI_Order_Type",
            "ShipFromCode", "ShipToCode", "Country", "Quantity", "PutAwayDate",
            "FY", "Quarter", "Month", "Week", "OrderType", "Count_RC", "Count_PO"
        ]
        insert_placeholders = ", ".join(["%s"] * len(insert_columns))
        update_placeholders = ", ".join([f"{col}=VALUES({col})" for col in insert_columns[1:]])

        insert_query = f"""
            INSERT INTO {RECEIVING_TAT_REPORT_TABLE} ({", ".join(insert_columns)}) 
            VALUES ({insert_placeholders})
            ON DUPLICATE KEY UPDATE {update_placeholders}
        """

        conn.executemany(
            insert_query,
            df[insert_columns].where(pd.notnull(df), None).values.tolist()
        )

def get_db_data():
    with MySQLConnectionPool() as conn:
        conn.execute_query(f"SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}")
        return pd.DataFrame(conn.cursor.fetchall(), columns=[i[0] for i in conn.cursor.description])
    
    
def create_tables():
    order_type_table = f"""
    CREATE TABLE IF NOT EXISTS {ORDER_TYPE_TABLE} (
        EDI_Order_Type VARCHAR(255) PRIMARY KEY,
        Detailed_Order_Type VARCHAR(255)
    )
    """
    receiving_tat_table = f"""
    CREATE TABLE IF NOT EXISTS {RECEIVING_TAT_REPORT_TABLE} (
        ReceiptNo VARCHAR(255) PRIMARY KEY,
        Replen_Balance_Order VARCHAR(255),
        Cust_Sys_No VARCHAR(255),
        Allocated_Part VARCHAR(255),
        EDI_Order_Type VARCHAR(255),
        ShipFromCode VARCHAR(255),
        ShipToCode VARCHAR(255),
        Country VARCHAR(255),
        Quantity BIGINT,
        PutAwayDate DATETIME,
        FY VARCHAR(20),
        Quarter VARCHAR(10),
        Month VARCHAR(2),
        Week VARCHAR(10),
        OrderType VARCHAR(255),
        Count_RC INT,
        Count_PO INT,
        FOREIGN KEY (EDI_Order_Type) REFERENCES OrderType(EDI_Order_Type)
    )
    """
    with MySQLConnectionPool() as conn:
        conn.execute_query(order_type_table)
        conn.execute_query(receiving_tat_table)
    print("테이블 생성 완료")