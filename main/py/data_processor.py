from queue import Full
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta, SA
import logging
import numpy as np

from typing import Tuple, Dict, Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            logger.info("Original column names: %s", df.columns.tolist())
            logger.info("First few rows of original data:\n%s", df.head().to_string())

            df = self._rename_columns(df)
            df = self._convert_data_types(df)
            df = self._clean_replen_balance_order(df)
            df = self._handle_ship_from_code(df)  # 새로 추가된 메서드 호출
            df = self._calculate_fiscal_data(df)
            df = self._map_order_type(df)
            df = self._handle_missing_values(df)
            df = self._calculate_count_po(df)
            df = df[df["Country"] == "KR"]

            logger.info("Data processing completed successfully")
            return df

        except Exception as e:
            logger.error("Error in process_dataframe: %s", str(e))
            raise

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.config["COLUMN_MAPPING"])

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Quantity" in df.columns:
            df["Quantity"] = (
                pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
            )

        date_columns = ["PutAwayDate", "ActualPhysicalReceiptDate"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "PutAwayDate" in df.columns and "ActualPhysicalReceiptDate" in df.columns:
            # PutAwayDate가 NaT(Null)인 경우 ActualPhysicalReceiptDate 값을 사용
            df.loc[df["PutAwayDate"].isnull(), "PutAwayDate"] = df.loc[
                df["PutAwayDate"].isnull(), "ActualPhysicalReceiptDate"
            ]

            # PutAwayDate에 시간 정보가 없는 경우 ActualPhysicalReceiptDate의 시간 정보를 사용
            mask = df["PutAwayDate"].dt.time == pd.Timestamp("00:00:00").time()
            df.loc[mask, "PutAwayDate"] = df.loc[mask, "PutAwayDate"].dt.date.astype(
                "datetime64[ns]"
            ) + pd.to_timedelta(
                df.loc[mask, "ActualPhysicalReceiptDate"].dt.time.astype(str)
            )

        if "PutAwayDate" in df.columns:
            df["InventoryDate"] = df["PutAwayDate"].dt.date

        return df

    def _clean_replen_balance_order(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Replen_Balance_Order" in df.columns:
            df["Replen_Balance_Order"] = df["Replen_Balance_Order"].astype(str)
            df["Replen_Balance_Order"] = df["Replen_Balance_Order"].apply(
                lambda x: x.split(".")[0] if "." in x else x
            )
            numeric_mask = df["Replen_Balance_Order"].str.isnumeric()
            df.loc[numeric_mask, "Replen_Balance_Order"] = pd.to_numeric(
                df.loc[numeric_mask, "Replen_Balance_Order"], errors="coerce"
            ).astype("Int64")
        return df

    def get_fy_start(self, date: datetime.date) -> datetime.date:
        if date.month < 2 or (date.month == 2 and date.day < 7):
            year = date.year - 1
        else:
            year = date.year
        feb_1 = datetime.date(year, 2, 1)
        return feb_1 + relativedelta(weekday=SA)

    def get_dell_week_and_fy(self, date: datetime.date) -> Tuple[str, str]:
        if pd.isna(date):
            return "Unknown", "Unknown"
        fy_start = self.get_fy_start(date)
        if date < fy_start:
            fy_start = self.get_fy_start(date.replace(year=date.year - 1))
            fy = fy_start.year
        else:
            fy = fy_start.year + 1
        days_since_fy_start = (date - fy_start).days
        dell_week = (days_since_fy_start // 7) + 1
        return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"

    def get_quarter(self, date: datetime.date) -> str:
        if pd.isna(date):
            return "Unknown"
        fy_start = self.get_fy_start(date)
        days_since_fy_start = (date - fy_start).days
        quarter = (days_since_fy_start // 91) + 1
        return f"Q{min(quarter, 4)}"

    def _calculate_fiscal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if "PutAwayDate" in df.columns:
            fiscal_data = df["PutAwayDate"].dt.date.apply(
                lambda x: (
                    self.get_dell_week_and_fy(x)
                    if pd.notna(x)
                    else ("Unknown", "Unknown")
                )
            )
            df["Week"] = fiscal_data.apply(lambda x: x[0])
            df["FY"] = fiscal_data.apply(lambda x: x[1])
            df["Quarter"] = df["PutAwayDate"].dt.date.apply(
                lambda x: self.get_quarter(x) if pd.notna(x) else "Unknown"
            )
            df["Month"] = df["PutAwayDate"].dt.strftime("%m").fillna("00")
        return df

    def _map_order_type(self, df: pd.DataFrame) -> pd.DataFrame:
        df["OrderType"] = df["EDI_Order_Type"].map(self.config["ORDER_TYPE_MAPPING"])
        df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("")
            elif df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(0)
        return df

    def _calculate_count_po(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Count_PO"] = df.groupby("Cust_Sys_No")["Cust_Sys_No"].transform("count")
        return df

    def _handle_ship_from_code(self, df: pd.DataFrame) -> pd.DataFrame:
        if "ShipFromCode" in df.columns:
            df.loc[df["ShipFromCode"].str.upper() == "REMARK", "ShipFromCode"] = np.nan
        return df


def main_data_processing(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    try:
        processor = DataProcessor(config)
        df = processor.process_dataframe(df)
        logger.info("Data processing completed successfully")
        return df
    except Exception as e:
        logger.error("Error in main_data_processing: %s", str(e))
        raise
