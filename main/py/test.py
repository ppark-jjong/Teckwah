import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, RECEIVING_TAT_REPORT_TABLE


def read_xlsb(filepath, sheet_name):
    import pyxlsb

    with pyxlsb.open_workbook(filepath) as wb:
        with wb.get_sheet(sheet_name) as sheet:
            data = [[c.v for c in r] for r in sheet.rows()]
    return pd.DataFrame(data[1:], columns=data[0])


def get_raw_data():
    raw_data_file = "C:/MyMain/test/Dashboard_Raw Data.xlsb"
    return read_xlsb(raw_data_file, "Receiving_TAT")


def get_db_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {RECEIVING_TAT_REPORT_TABLE}")
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            cursor.close()
            conn.close()
            print(f"Successfully fetched {len(df)} records from database.")
            return df
        else:
            print("Failed to connect to the database.")
            return pd.DataFrame()
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return pd.DataFrame()


def compare_data(raw_df, db_df):
    print(f"Raw data shape: {raw_df.shape}")
    print(f"DB data shape: {db_df.shape}")

    if db_df.empty:
        print(
            "No data fetched from database. Please check the database connection and table."
        )
        return

    # Ensure column names match
    raw_df = raw_df.rename(
        columns={
            "Replen/Balance Order#": "Replen_Balance_Order",
            "Cust Sys No": "Cust_Sys_No",
        }
    )

    # Create composite keys
    raw_df["composite_key"] = (
        raw_df["ReceiptNo"].astype(str)
        + "|"
        + raw_df["Replen_Balance_Order"].astype(str)
        + "|"
        + raw_df["Cust_Sys_No"].astype(str)
    )

    db_df["composite_key"] = (
        db_df["ReceiptNo"].astype(str)
        + "|"
        + db_df["Replen_Balance_Order"].astype(str)
        + "|"
        + db_df["Cust_Sys_No"].astype(str)
    )

    # Check for duplicates in raw data
    raw_duplicates = raw_df[raw_df.duplicated("composite_key", keep=False)]
    print(f"Duplicates in raw data: {len(raw_duplicates)}")

    # Check for duplicates in DB data
    db_duplicates = db_df[db_df.duplicated("composite_key", keep=False)]
    print(f"Duplicates in DB data: {len(db_duplicates)}")

    # Check for missing records in DB
    missing_in_db = raw_df[~raw_df["composite_key"].isin(db_df["composite_key"])]
    print(f"Records in raw data missing from DB: {len(missing_in_db)}")

    # Check for extra records in DB
    extra_in_db = db_df[~db_df["composite_key"].isin(raw_df["composite_key"])]
    print(f"Extra records in DB not in raw data: {len(extra_in_db)}")

    # Detailed report
    if len(raw_duplicates) > 0:
        print("\nSample of duplicates in raw data:")
        print(
            raw_duplicates[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head()
        )

    if len(missing_in_db) > 0:
        print("\nSample of records missing in DB:")
        print(
            missing_in_db[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head()
        )

    if len(extra_in_db) > 0:
        print("\nSample of extra records in DB:")
        print(extra_in_db[["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]].head())


def main():
    raw_df = get_raw_data()
    db_df = get_db_data()
    compare_data(raw_df, db_df)


if __name__ == "__main__":
    main()
