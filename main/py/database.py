import mysql.connector
from mysql.connector import pooling
import pandas as pd
from typing import List, Dict, Any
import logging
from config import (
    DB_CONFIG,
    POOL_NAME,
    POOL_SIZE,
    ORDER_TYPE_TABLE,
    RECEIVING_TAT_REPORT_TABLE,
    ORDER_TYPE_MAPPING,
)

logger = logging.getLogger(__name__)


class MySQLConnectionPool:
    def __init__(self):
        """
        MySQL 연결 풀을 생성합니다.
        """
        try:
            self.pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=POOL_NAME, pool_size=POOL_SIZE, **DB_CONFIG
            )
            logger.info("Connection pool created successfully")
        except mysql.connector.Error as err:
            logger.error(f"Error creating connection pool: {err}")
            raise

    def __enter__(self):
        """
        풀에서 연결을 가져옵니다.
        """
        try:
            self.connection = self.pool.get_connection()
            self.cursor = self.connection.cursor(buffered=True)
            logger.info("Connection acquired from pool")
            return self
        except mysql.connector.Error as err:
            logger.error(f"Error acquiring connection from pool: {err}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        연결을 반환합니다.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Connection returned to pool")

    def execute_query(self, query: str, params: tuple = None):
        """
        쿼리를 실행합니다.
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except mysql.connector.Error as err:
            logger.error(f"Error executing query: {err}")
            self.connection.rollback()
            raise

    def executemany(self, query: str, params: List[tuple]):
        """
        여러 쿼리를 배치 실행합니다.
        """
        try:
            self.cursor.executemany(query, params)
            self.connection.commit()
        except mysql.connector.Error as err:
            logger.error(f"Error executing batch query: {err}")
            self.connection.rollback()
            raise


def create_tables():
    """
    필요한 데이터베이스 테이블을 생성합니다.
    """
    order_type_table = f"""
    CREATE TABLE IF NOT EXISTS {ORDER_TYPE_TABLE} (
        EDI_Order_Type VARCHAR(255) PRIMARY KEY,
        Detailed_Order_Type VARCHAR(255)
    )
    """
    receiving_tat_table = f"""
    CREATE TABLE IF NOT EXISTS {RECEIVING_TAT_REPORT_TABLE} (
        ReceiptNo VARCHAR(255),
        Replen_Balance_Order BIGINT,
        Cust_Sys_No VARCHAR(255),
        Allocated_Part VARCHAR(255),
        EDI_Order_Type VARCHAR(255),
        ShipFromCode VARCHAR(255),
        ShipToCode VARCHAR(255),
        Country VARCHAR(255),
        Quantity BIGINT,
        PutAwayDate DATETIME,
        InventoryDate DATE,
        FY VARCHAR(20),
        Quarter VARCHAR(10),
        Month VARCHAR(2),
        Week VARCHAR(10),
        OrderType VARCHAR(255),
        Count_PO INT,
        PRIMARY KEY (ReceiptNo, Replen_Balance_Order, Cust_Sys_No),
        FOREIGN KEY (EDI_Order_Type) REFERENCES {ORDER_TYPE_TABLE}(EDI_Order_Type)
    )
    """
    with MySQLConnectionPool() as conn:
        conn.execute_query(order_type_table)
        conn.execute_query(receiving_tat_table)
    logger.info("Tables created successfully")


def upload_to_mysql(df: pd.DataFrame):
    """
    데이터프레임의 데이터를 데이터베이스에 업로드합니다.
    """
    df = df[df["Country"] == "KR"]

    with MySQLConnectionPool() as conn:
        for edi_type, detailed_type in ORDER_TYPE_MAPPING.items():
            query = f"""
            INSERT INTO {ORDER_TYPE_TABLE} (EDI_Order_Type, Detailed_Order_Type)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Detailed_Order_Type=VALUES(Detailed_Order_Type)
            """
            conn.execute_query(query, (edi_type, detailed_type))

        insert_columns = [
            "ReceiptNo",
            "Replen_Balance_Order",
            "Cust_Sys_No",
            "Allocated_Part",
            "EDI_Order_Type",
            "ShipFromCode",
            "ShipToCode",
            "Country",
            "Quantity",
            "PutAwayDate",
            "InventoryDate",
            "FY",
            "Quarter",
            "Month",
            "Week",
            "OrderType",
            "Count_PO",
        ]

        existing_columns = [col for col in insert_columns if col in df.columns]

        insert_placeholders = ", ".join(["%s"] * len(existing_columns))
        update_placeholders = ", ".join(
            [
                f"{col}=VALUES({col})"
                for col in existing_columns
                if col not in ["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]
            ]
        )

        insert_query = f"""
        INSERT INTO {RECEIVING_TAT_REPORT_TABLE} ({", ".join(existing_columns)}) 
        VALUES ({insert_placeholders})
        ON DUPLICATE KEY UPDATE {update_placeholders}
        """

        df_to_insert = df.reindex(columns=existing_columns, fill_value=None)

        datetime_cols = ["PutAwayDate", "InventoryDate"]
        for col in datetime_cols:
            if col in df_to_insert.columns:
                df_to_insert[col] = pd.to_datetime(df_to_insert[col]).dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

        if "Replen_Balance_Order" in df_to_insert.columns:
            df_to_insert["Replen_Balance_Order"] = df_to_insert[
                "Replen_Balance_Order"
            ].astype(str)

        df_to_insert = df_to_insert.where(pd.notna(df_to_insert), None)

        data_to_insert = df_to_insert.values.tolist()

        conn.executemany(insert_query, data_to_insert)

    logger.info(f"{len(data_to_insert)} rows uploaded to database")


def get_db_data() -> pd.DataFrame:
    """
    데이터베이스에서 모든 데이터를 가져옵니다.
    """
    try:
        with MySQLConnectionPool() as conn:
            query = f"SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}"
            conn.execute_query(query)
            result = conn.cursor.fetchall()
            columns = [i[0] for i in conn.cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        logger.error(f"Error fetching data from database: {str(e)}")
        return pd.DataFrame()


def get_data_by_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    지정된 기간 동안의 데이터를 가져옵니다.
    """
    with MySQLConnectionPool() as conn:
        query = f"""
        SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}
        WHERE PutAwayDate BETWEEN %s AND %s
        """
        conn.execute_query(query, (start_date, end_date))
        result = conn.cursor.fetchall()
        columns = [i[0] for i in conn.cursor.description]
        return pd.DataFrame(result, columns=columns)


def get_data_by_inventory_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    지정된 기간 동안의 데이터를 inventoryDate를 기준으로 추출합니다.
    """
    query = f"""
    SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}
    WHERE InventoryDate BETWEEN %s AND %s
    """
    with MySQLConnectionPool() as conn:
        conn.execute_query(query, (start_date, end_date))
        result = conn.cursor.fetchall()
        columns = [i[0] for i in conn.cursor.description]
        return pd.DataFrame(result, columns=columns)


def get_data_by_fy_and_quarter(fy: str, quarter: str) -> pd.DataFrame:
    """
    지정된 FY와 Quarter에 해당하는 데이터를 데이터베이스에서 추출합니다.

    :param fy: 회계연도 (예: 'FY23')
    :param quarter: 분기 (예: 'Q1', 'Q2')
    :return: 추출된 데이터를 포함하는 DataFrame
    """
    query = f"""
    SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}
    WHERE FY = %s AND Quarter = %s
    """
    try:
        with MySQLConnectionPool() as conn:
            conn.execute_query(query, (fy, quarter))
            result = conn.cursor.fetchall()
            columns = [i[0] for i in conn.cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        logger.error(f"데이터베이스에서 데이터를 가져오는 중 오류 발생: {str(e)}")
        return pd.DataFrame()  # 빈 DataFrame 반환
