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

    # 컬럼 이름 변경 (실제 존재하는 컬럼만)
    column_mapping = {
        "ReceiptNo": "ReceiptNo",
        "Replen/Balance Order#": "Replen_Balance_Order",
        "Cust Sys No": "Cust_Sys_No",
        "Allocated Part#": "Allocated_Part",
        "EDI Order Type": "EDI_Order_Type",
        "ShipFromCode": "ShipFromCode",
        "ShipToCode": "ShipToCode",
        "Country": "Country",
        "Region": "Region",
        "Quantity": "Quantity",
        "ActualPhysicalReceiptDate": "ActualPhysicalReceiptDate",
        "PutAwayDate": "PutAwayDate",
        "Meet KPI": "Meet_KPI",
        "PutAwayDate - ActualPhysicalReceiptDate (In Mins)": "PutAway_Receipt_Diff_Mins",
        "Inbound KPI": "Inbound_KPI",
    }
    df = df.rename(
        columns={
            col: column_mapping[col] for col in df.columns if col in column_mapping
        }
    )

    # 변경 후 컬럼 이름 출력
    print("Renamed column names:", df.columns.tolist())

    # 데이터 타입 변환 및 NULL 처리
    if "Quantity" in df.columns:
        df["Quantity"] = (
            pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
        )
    if "PutAwayDate" in df.columns:
        df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors="coerce")
    if "ActualPhysicalReceiptDate" in df.columns:
        df["ActualPhysicalReceiptDate"] = pd.to_datetime(
            df["ActualPhysicalReceiptDate"], errors="coerce"
        )

    # 날짜 관련 컬럼 추가 (PutAwayDate 기준)
    if "PutAwayDate" in df.columns:
        df["FY"] = df["PutAwayDate"].apply(
            lambda x: f"FY{x.year % 100:02d}" if pd.notnull(x) else None
        )
        df["Week"] = df["PutAwayDate"].dt.strftime("WK%U")
        df["Quarter"] = df["PutAwayDate"].dt.to_period("Q").dt.strftime("Q%q")
        df["Month"] = df["PutAwayDate"].dt.strftime("%m")

    # CountPO 계산 (ReceiptNo 기준)
    df["Count_PO"] = df.groupby("ReceiptNo")["ReceiptNo"].transform("count")

    # CountRC 계산 (Cust_Sys_No 기준)
    df["Count_RC"] = df.groupby("Cust_Sys_No")["Cust_Sys_No"].transform("count")

    # OrderType 컬럼 추가 (EDI Order Type 기준)
    df["OrderType"] = df["EDI_Order_Type"].map(ORDER_TYPE_MAPPING)
    df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"

    # NULL 값 처리
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")
        elif df[col].dtype in ["int64", "float64"]:
            df[col] = df[col].fillna(0)

    return df
