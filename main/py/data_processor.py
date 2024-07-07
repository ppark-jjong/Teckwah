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
            "Count RC": "Count_RC",
            "Count PO": "Count_PO",
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
        "Count_RC",
        "Count_PO",
    ]

    # 실제 존재하는 열만 선택
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[existing_columns]

    # 데이터 타입 변환
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")
    if "PutAwayDate" in df.columns:
        df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors="coerce")
    if "Count_RC" in df.columns:
        df["Count_RC"] = pd.to_numeric(df["Count_RC"], errors="coerce").astype("Int32")
    if "Count_PO" in df.columns:
        df["Count_PO"] = pd.to_numeric(df["Count_PO"], errors="coerce").astype("Int32")

    # 'Week' 열이 없다면 'Dell-Week'에서 생성
    if "Week" not in df.columns and "Dell-Week" in df.columns:
        df["Week"] = df["Dell-Week"]
    # 'Month' 열을 2자리 숫자 형식으로 변환
    if "Month" in df.columns:
        df["Month"] = (
            pd.to_numeric(df["Month"], errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )
    elif "M" in df.columns:  # 'M' 열이 있다면 이를 'Month'로 사용
        df["Month"] = (
            pd.to_numeric(df["M"], errors="coerce")
            .fillna(0)
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )
    else:
        df["Month"] = df["PutAwayDate"].dt.strftime("%m")  # PutAwayDate에서 월 추출

    # 'Count_RC'와 'Count_PO' 열이 없다면 생성
    if "Count_RC" not in df.columns:
        df["Count_RC"] = 1
    if "Count_PO" not in df.columns:
        df["Count_PO"] = 1

    return df
