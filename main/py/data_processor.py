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
    try:
        df = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)
        df = df[df["Country"] == "KR"].copy()
        df["PutAwayDate"] = pd.to_datetime(
            df["PutAwayDate"], format="%m/%d/%Y %H:%M:%S", errors="coerce"
        )

        df["FY"] = df["PutAwayDate"].apply(
            lambda x: get_dell_week_and_fy(x)[1] if pd.notnull(x) else None
        )
        df["Quarter"] = df["PutAwayDate"].apply(
            lambda x: get_quarter(x) if pd.notnull(x) else None
        )
        df["Month"] = df["PutAwayDate"].dt.strftime("%m")
        df["Week"] = df["PutAwayDate"].apply(
            lambda x: get_dell_week_and_fy(x)[0] if pd.notnull(x) else None
        )
        df["OrderType"] = df["EDI_Order_Type"].map(ORDER_TYPE_MAPPING)

        df["Count_RC"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])[
            "ReceiptNo"
        ].transform("count")
        df["Count_PO"] = df.groupby(["ReceiptNo", "EDI_Order_Type"])[
            "Replen_Balance_Order"
        ].transform("count")

        return df
    except Exception as e:
        print(f"데이터프레임 처리 중 오류 발생: {str(e)}")
        raise
