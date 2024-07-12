import pandas as pd
import datetime
import logging
from typing import Tuple, Dict, Any, Optional

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def get_fy_start(self, date: datetime.datetime) -> datetime.datetime:
        """
        Dell의 회계연도 시작일(2월 첫 번째 토요일)을 계산합니다.

        :param date: 기준 날짜
        :return: 회계연도 시작일
        """
        if pd.isna(date):
            return pd.NaT
        year = date.year
        feb_1 = datetime.datetime(year, 2, 1)
        days_to_saturday = (5 - feb_1.weekday()) % 7
        return feb_1 + datetime.timedelta(days=days_to_saturday)

    def get_dell_week_and_fy(self, date: datetime.datetime) -> Tuple[str, str]:
        """
        Dell의 주(Week)와 회계연도(FY)를 계산합니다.

        :param date: 기준 날짜
        :return: (Week, FY) 형태의 튜플
        """
        if pd.isna(date):
            return "Unknown", "Unknown"

        fy_start = self.get_fy_start(date)
        if date < fy_start:
            fy_start = self.get_fy_start(date.replace(year=date.year - 1))
            fy = fy_start.year
        else:
            fy = fy_start.year

        days_since_fy_start = (date - fy_start).days
        dell_week = min((days_since_fy_start // 7) + 1, 52)

        return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"

    def get_quarter(self, date: datetime.datetime) -> str:
        """
        분기(Quarter)를 계산합니다.

        :param date: 기준 날짜
        :return: 분기 문자열 (예: "Q1")
        """
        if pd.isna(date):
            return "Unknown"
        _, fy = self.get_dell_week_and_fy(date)
        fy_year = int(fy[2:]) + 2000
        fy_start = self.get_fy_start(datetime.datetime(fy_year - 1, 2, 1))
        quarter = min((date - fy_start).days // 91 + 1, 4)
        return f"Q{quarter}"

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터프레임을 처리합니다.

        :param df: 원본 데이터프레임
        :return: 처리된 데이터프레임
        """
        try:
            logger.info("Original column names: %s", df.columns.tolist())
            logger.info("First few rows of original data:\n%s", df.head().to_string())

            df = self._rename_columns(df)
            df = self._convert_data_types(df)
            df = self._calculate_fiscal_data(df)
            df = self._map_order_type(df)
            df = self._handle_missing_values(df)
            df = self._calculate_count_po(df)

            logger.info("Data processing completed successfully")
            return df

        except Exception as e:
            logger.error("Error in process_dataframe: %s", str(e))
            raise

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """열 이름을 변경합니다."""
        return df.rename(
            columns={
                col: self.config["COLUMN_MAPPING"].get(col, col) for col in df.columns
            }
        )

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 타입을 변환합니다."""
        if "Quantity" in df.columns:
            df["Quantity"] = (
                pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")
            )
        if "PutAwayDate" in df.columns:
            df["PutAwayDate"] = pd.to_datetime(df["PutAwayDate"], errors="coerce")
        return df

    def _calculate_fiscal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """회계 관련 데이터를 계산합니다."""
        if "PutAwayDate" in df.columns:
            fiscal_data = df["PutAwayDate"].apply(
                lambda x: (
                    self.get_dell_week_and_fy(x)
                    if pd.notna(x)
                    else ("Unknown", "Unknown")
                )
            )
            df["FY"] = fiscal_data.apply(lambda x: x[1])
            df["Week"] = fiscal_data.apply(lambda x: x[0])
            df["Quarter"] = df["PutAwayDate"].apply(
                lambda x: self.get_quarter(x) if pd.notna(x) else "Unknown"
            )
            df["Month"] = df["PutAwayDate"].dt.strftime("%m").fillna("Unknown")
        return df

    def _map_order_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """주문 유형을 매핑합니다."""
        df["OrderType"] = df["EDI_Order_Type"].map(self.config["ORDER_TYPE_MAPPING"])
        df.loc[df["OrderType"].isna(), "OrderType"] = "Unknown"
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """결측값을 처리합니다."""
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("")
            elif df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(0)
        return df

    def _calculate_count_po(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cust Sys No를 기준으로 Count_PO를 계산합니다."""
        df["Count_PO"] = df.groupby("Cust_Sys_No")["Cust_Sys_No"].transform("count")
        return df


def main_data_processing(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    메인 데이터 처리 함수입니다.

    :param df: 처리할 데이터프레임
    :param config: 설정 정보
    :return: 처리된 데이터프레임
    """
    try:
        processor = DataProcessor(config)
        df = processor.process_dataframe(df)
        logger.info("Data processing completed successfully")
        return df
    except Exception as e:
        logger.error("Error in main_data_processing: %s", str(e))
        raise


# 테스트 코드는 별도의 test_data_processor.py 파일로 이동
