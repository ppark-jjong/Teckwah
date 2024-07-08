import pandas as pd
import datetime
from config import COLUMN_MAPPING, ORDER_TYPE_MAPPING


def get_fy_start(year):
    first_day = datetime.datetime(year, 2, 1)
    return first_day + datetime.timedelta(days=(5 - first_day.weekday() + 7) % 7)


def get_dell_week_and_fy(date):
    fy_start = get_fy_start(
        date.year if date >= get_fy_start(date.year) else date.year - 1
    )
    fy = date.year + 1 if date >= fy_start else date.year
    dell_week = min((date - fy_start).days // 7 + 1, 52)
    return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"


def get_quarter(date):
    _, fy = get_dell_week_and_fy(date)
    fy_year = int(fy[2:]) + 2000
    fy_start = get_fy_start(fy_year - 1)
    quarter = min((date - fy_start).days // 91 + 1, 4)
    return f"Q{quarter}"


def process_dataframe(df):
    # 원본 컬럼 이름 출력
    print("Original column names:", df.columns.tolist())

    # 컬럼 이름 변경
    column_mapping = {
        "Replen/Balance Order#": "Replen_Balance_Order",
        "Cust Sys No": "Cust_Sys_No",
        "Allocated Part#": "Allocated_Part",
        "EDI Order Type": "EDI_Order_Type",
        "Ship From": "ShipFromCode",
        "Ship to": "ShipToCode",
        "Dell-Week": "Week",
        "Order Type": "OrderType",
        "Count RC": "Count_RC",
        "Count PO": "Count_PO",
    }
    df = df.rename(columns=column_mapping)

    # 변경 후 컬럼 이름 출력
    print("Renamed column names:", df.columns.tolist())

    # 필요한 컬럼만 선택
    columns_to_keep = [
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
        "FY",
        "Quarter",
        "Month",
        "Week",
        "OrderType",
        "Count_RC",
        "Count_PO",
    ]

    # 실제 존재하는 열만 선택
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[existing_columns]

    # 데이터 타입 변환 및 NULL 처리
    if "Quantity" in df.columns:
        df["Quantity"] = (
            pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
        )
    if "PutAwayDate" in df.columns:
        df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors="coerce")
    if "Count_RC" in df.columns:
        df["Count_RC"] = (
            pd.to_numeric(df["Count_RC"], errors="coerce").fillna(0).astype("Int32")
        )
    if "Count_PO" in df.columns:
        df["Count_PO"] = (
            pd.to_numeric(df["Count_PO"], errors="coerce").fillna(0).astype("Int32")
        )

    # 'FY' 열 처리
    if "FY" not in df.columns and "PutAwayDate" in df.columns:
        df["FY"] = df["PutAwayDate"].apply(
            lambda x: f"FY{x.year % 100:02d}" if pd.notnull(x) else None
        )

    # 'Month' 열 처리
    if "Month" in df.columns:
        df["Month"] = (
            pd.to_numeric(df["Month"], errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )
    elif "M" in df.columns:
        df["Month"] = (
            pd.to_numeric(df["M"], errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )
    elif "PutAwayDate" in df.columns:
        df["Month"] = df["PutAwayDate"].dt.strftime("%m")

    # NULL 값 처리
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")
        elif df[col].dtype in ["int64", "float64"]:
            df[col] = df[col].fillna(0)

    # 처리된 데이터의 샘플 출력
    print("Processed data sample:")
    print(df.head())
    print("\nColumn info:")
    print(df.info())

    return df
