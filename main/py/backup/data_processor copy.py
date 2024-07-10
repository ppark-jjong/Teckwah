import pandas as pd
import datetime
import logging
from config import COLUMN_MAPPING, ORDER_TYPE_MAPPING

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_fy_start(date):
    if pd.isna(date):
        return pd.NaT
    year = date.year
    feb_1 = datetime.datetime(year, 2, 1)
    days_to_saturday = (5 - feb_1.weekday() + 7) % 7
    return feb_1 + datetime.timedelta(days=days_to_saturday)


def get_dell_week_and_fy(date):
    if pd.isna(date):
        return "Unknown", "Unknown"

    fy_start = get_fy_start(date)
    if date < fy_start:
        fy_start = get_fy_start(date.replace(year=date.year - 1))
        fy = fy_start.year
    else:
        fy = fy_start.year + 1

    days_since_fy_start = (date - fy_start).days
    dell_week = (days_since_fy_start // 7) + 1

    # 52주로 제한
    if dell_week > 52:
        dell_week = 52

    return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"


def get_quarter(date):
    if pd.isna(date):
        return "Unknown"
    _, fy = get_dell_week_and_fy(date)
    fy_year = int(fy[2:]) + 2000
    fy_start = get_fy_start(datetime.datetime(fy_year - 1, 2, 1))
    quarter = min((date - fy_start).days // 91 + 1, 4)
    return f"Q{quarter}"


def process_dataframe(df):
    try:
        logging.info("Original column names: %s", df.columns.tolist())
        logging.info("First few rows of original data:\n%s", df.head().to_string())

        df = df.rename(
            columns={col: COLUMN_MAPPING.get(col, col) for col in df.columns}
        )

        logging.info("Renamed column names: %s", df.columns.tolist())
        logging.info("First few rows after renaming:\n%s", df.head().to_string())

        if "Quantity" in df.columns:
            df["Quantity"] = (
                pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
            )

        if "PutAwayDate" in df.columns:
            logging.info(
                "PutAwayDate column data type before conversion: %s",
                df["PutAwayDate"].dtype,
            )
            logging.info(
                "PutAwayDate column first few values before conversion: %s",
                df["PutAwayDate"].head(),
            )

            df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors="coerce")

            logging.info(
                "PutAwayDate column data type after conversion: %s",
                df["PutAwayDate"].dtype,
            )
            logging.info(
                "PutAwayDate column first few values after conversion: %s",
                df["PutAwayDate"].head(),
            )

            fiscal_data = df["PutAwayDate"].apply(
                lambda x: (
                    get_dell_week_and_fy(x) if pd.notna(x) else ("Unknown", "Unknown")
                )
            )
            df["FY"] = fiscal_data.apply(lambda x: x[1])
            df["Week"] = fiscal_data.apply(lambda x: x[0])
            df["Quarter"] = df["PutAwayDate"].apply(
                lambda x: get_quarter(x) if pd.notna(x) else "Unknown"
            )
            df["Month"] = df["PutAwayDate"].dt.strftime("%m").fillna("Unknown")

        df["OrderType"] = df["EDI_Order_Type"].map(ORDER_TYPE_MAPPING)
        df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"

        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("")
            elif df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(0)

        logging.info("Data processing completed successfully")

        df["Count_PO"] = df.groupby(
            ["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No"]
        )["ReceiptNo"].transform("count")

        logging.info("Data processing completed successfully")
        return df

    except Exception as e:
        logging.error("Error in process_dataframe: %s", str(e))
        raise


def main_data_processing(df):
    try:
        df = process_dataframe(df)
        logging.info("Data processing completed successfully")
        return df
    except Exception as e:
        logging.error("Error in main_data_processing: %s", str(e))
        raise


if __name__ == "__main__":
    test_df = pd.read_excel("path_to_your_test_file.xlsx")
    processed_df = main_data_processing(test_df)
    print(processed_df.head())
