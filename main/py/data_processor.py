import pandas as pd
import datetime
import logging
from config import COLUMN_MAPPING, ORDER_TYPE_MAPPING

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_fy_start(date):
    if pd.isna(date):
        return pd.NaT
    year = date.year
    feb_1 = datetime.datetime(year, 2, 1)
    days_to_saturday = (5 - feb_1.weekday() + 7) % 7
    return feb_1 + datetime.timedelta(days=days_to_saturday)  # 토요일이 회계연도 시작

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
    
    # 금요일이 지나지 않았다면 이전 주로 계산
    if date.weekday() < 5:  # 0:월, 1:화, 2:수, 3:목, 4:금, 5:토, 6:일
        dell_week -= 1
    
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
        # 원본 컬럼 이름 출력
        logging.info("Original column names: %s", df.columns.tolist())

        # 컬럼 이름 변경 (실제 존재하는 컬럼만)
        df = df.rename(columns={col: COLUMN_MAPPING[col] for col in df.columns if col in COLUMN_MAPPING})

        # 변경 후 컬럼 이름 출력
        logging.info("Renamed column names: %s", df.columns.tolist())

        # 데이터 타입 변환 및 NULL 처리
        if "Quantity" in df.columns:
            df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")

        if "PutAwayDate" in df.columns:
            logging.info("PutAwayDate column data type before conversion: %s", df["PutAwayDate"].dtype)
            logging.info("PutAwayDate column first few values before conversion: %s", df["PutAwayDate"].head())

            df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors='coerce')

            logging.info("PutAwayDate column data type after conversion: %s", df["PutAwayDate"].dtype)
            logging.info("PutAwayDate column first few values after conversion: %s", df["PutAwayDate"].head())

            fiscal_data = df["PutAwayDate"].apply(lambda x: get_dell_week_and_fy(x) if pd.notna(x) else ("Unknown", "Unknown"))
            df["FY"] = fiscal_data.apply(lambda x: x[1])
            df["Week"] = fiscal_data.apply(lambda x: x[0])
            df["Quarter"] = df["PutAwayDate"].apply(lambda x: get_quarter(x) if pd.notna(x) else "Unknown")
            df["Month"] = df["PutAwayDate"].dt.strftime("%m").fillna("Unknown")

        # CountPO 계산 (Cust_Sys_No 기준으로 현재 행까지의 동일 값 개수)
        df['CountPO'] = df.apply(lambda x: df.loc[:x.name, 'Cust_Sys_No'].value_counts()[x['Cust_Sys_No']], axis=1)

        # CountRC 계산 (ReceiptNo 기준으로 현재 행까지의 동일 값 개수)
        df['CountRC'] = df.apply(lambda x: df.loc[:x.name, 'ReceiptNo'].value_counts()[x['ReceiptNo']], axis=1)

        # OrderType 컬럼 추가 (EDI Order Type 기준)
        df["OrderType"] = df["EDI_Order_Type"].map(ORDER_TYPE_MAPPING)
        df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"

        # NULL 값 처리
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("")
            elif df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(0)

        logging.info("Data processing completed successfully")
        return df

    except Exception as e:
        logging.error("Error in process_dataframe: %s", str(e))
        raise