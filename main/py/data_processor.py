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
        """
        DataProcessor 클래스를 초기화합니다.
        """
        self.config = config

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터프레임을 처리합니다.
        """
        try:
            logger.info("Original column names: %s", df.columns.tolist())
            logger.info("First few rows of original data:\n%s", df.head().to_string())

            df = self._rename_columns(df)
            df = self._convert_data_types(df)
            df = self._clean_replen_balance_order(df)
            df = self._handle_ship_from_code(df)
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
        """
        컬럼 이름을 변경합니다.
        """
        return df.rename(columns=self.config["COLUMN_MAPPING"])

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 타입을 변환합니다.
        """
        if "Quantity" in df.columns:
            df["Quantity"] = (
                pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
            )

        date_columns = ["PutAwayDate", "ActualPhysicalReceiptDate"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "PutAwayDate" in df.columns and "ActualPhysicalReceiptDate" in df.columns:
            df.loc[df["PutAwayDate"].isnull(), "PutAwayDate"] = df.loc[
                df["PutAwayDate"].isnull(), "ActualPhysicalReceiptDate"
            ]

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
        """
        Replen_Balance_Order 컬럼을 정리합니다.
        """
        if "Replen_Balance_Order" in df.columns:
            df["Replen_Balance_Order"] = df["Replen_Balance_Order"].astype(str)
            df["Replen_Balance_Order"] = df["Replen_Balance_Order"].apply(
                lambda x: x.split(".")[0] if "." in x else x
            )
            numeric_mask = df["Replen_Balance_Order"].str.isnumeric()
            df.loc[numeric_mask, "Replen_Balance_Order"] = pd.to_numeric(
                df.loc[numeric_mask, "Replen_Balance_Order"], errors="coerce"
            ).astype("int64")
        return df

    def get_fy_start(self, date: datetime.date) -> datetime.date:
        """
        회계 연도 시작일을 계산합니다.
        """
        if date.month < 2 or (date.month == 2 and date.day < 7):
            year = date.year - 1
        else:
            year = date.year
        feb_1 = datetime.date(year, 2, 1)
        return feb_1 + relativedelta(weekday=SA)

    def get_dell_week_and_fy(self, date: datetime.date) -> Tuple[str, str]:
        """
        Dell 주와 회계 연도를 계산합니다.
        """
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
        """
        분기를 계산합니다.
        """
        if pd.isna(date):
            return "Unknown"
        fy_start = self.get_fy_start(date)
        days_since_fy_start = (date - fy_start).days
        quarter = (days_since_fy_start // 91) + 1
        return f"Q{min(quarter, 4)}"

    def _calculate_fiscal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        회계 데이터를 계산합니다.
        """
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
        """
        주문 타입을 매핑합니다.
        """
        df["OrderType"] = df["EDI_Order_Type"].map(self.config["ORDER_TYPE_MAPPING"])
        df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        결측값을 처리합니다.
        """
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].fillna("")
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
        return df

    def _calculate_count_po(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Count_PO 컬럼을 계산하고 Quantity를 합산합니다.
        """
        grouped = (
            df.groupby("Cust_Sys_No")
            .agg({"Cust_Sys_No": "count", "Quantity": "sum"})
            .rename(columns={"Cust_Sys_No": "Count_PO"})
        )

        df = df.merge(grouped, on="Cust_Sys_No", suffixes=("", "_sum"))

        df["Quantity"] = df["Quantity_sum"]
        df = df.drop(columns=["Quantity_sum"])

        return df

    def _handle_ship_from_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ShipFromCode 컬럼을 처리합니다.
        """
        if "ShipFromCode" in df.columns:
            df.loc[df["ShipFromCode"].str.upper() == "REMARK", "ShipFromCode"] = np.nan
        return df


def main_data_processing(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    DataProcessor 클래스를 이용해 데이터 프레임을 처리합니다.
    """
    try:
        processor = DataProcessor(config)
        df = processor.process_dataframe(df)
        logger.info("Data processing completed successfully")
        return df
    except Exception as e:
        logger.error("Error in main_data_processing: %s", str(e))
        raise
