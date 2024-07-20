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
    # 컬럼 이름 변경
    df = df.rename(
        columns={
            "Customer Order No": "Replen_Balance_Order",
            "PO No": "Cust_Sys_No",
            "Part": "Allocated_Part",
            "EDI Order Type": "EDI_Order_Type",
            "Ship From": "ShipFromCode",
            "Ship to": "ShipToCode",
            "Dell-Week": "Week",
            "Order Type": "OrderType",
        }
    )

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
        "Count RC",
        "Count PO",
    ]
    df = df[columns_to_keep]

    # 데이터 타입 변환
    df["Quantity"] = df["Quantity"].astype("int64")
    df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"])
    df["Count RC"] = df["Count RC"].astype("int32")
    df["Count PO"] = df["Count PO"].astype("int32")

    return df
