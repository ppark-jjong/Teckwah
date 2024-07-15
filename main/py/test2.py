import pandas as pd
import numpy as np
from database import get_db_data
from config import COLUMN_MAPPING
import os
import logging
import argparse
from datetime import datetime, date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_raw_data(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name="CS Receiving TAT")
        logger.info(f"Original columns: {df.columns.tolist()}")
        df.rename(columns=COLUMN_MAPPING, inplace=True)
        logger.info(f"Columns after renaming: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Raw 데이터 로드 중 오류 발생: {str(e)}")
        return None


def compare_data(raw_df, db_df, start_date, end_date):
    # 날짜 필터링
    raw_df["PutAwayDate"] = pd.to_datetime(raw_df["PutAwayDate"])
    db_df["PutAwayDate"] = pd.to_datetime(db_df["PutAwayDate"])

    raw_df = raw_df[
        (raw_df["PutAwayDate"].dt.date >= start_date)
        & (raw_df["PutAwayDate"].dt.date <= end_date)
    ]
    db_df = db_df[
        (db_df["PutAwayDate"].dt.date >= start_date)
        & (db_df["PutAwayDate"].dt.date <= end_date)
    ]

    # 공통 컬럼 찾기
    raw_columns = set(raw_df.columns)
    db_columns = set(db_df.columns)
    common_columns = list(raw_columns.intersection(db_columns))

    logger.info(f"Raw data unique columns: {raw_columns - db_columns}")
    logger.info(f"DB data unique columns: {db_columns - raw_columns}")
    logger.info(f"Common columns: {common_columns}")

    # 공통 컬럼만 선택
    raw_df = raw_df[common_columns]
    db_df = db_df[common_columns]

    # 데이터 정렬
    sort_columns = ["ReceiptNo", "Replen_Balance_Order", "Cust_Sys_No", "PutAwayDate"]
    raw_df = raw_df.sort_values(by=sort_columns)
    db_df = db_df.sort_values(by=sort_columns)

    # 인덱스 리셋
    raw_df = raw_df.reset_index(drop=True)
    db_df = db_df.reset_index(drop=True)

    # 데이터 길이 맞추기
    min_len = min(len(raw_df), len(db_df))
    raw_df = raw_df.head(min_len)
    db_df = db_df.head(min_len)

    # 데이터 비교
    comparison_df = pd.DataFrame()
    for column in common_columns:
        if column == "PutAwayDate":
            # 시간 데이터 비교 (초 단위까지만 비교)
            mask = raw_df[column].dt.round("S") != db_df[column].dt.round("S")
        else:
            mask = raw_df[column] != db_df[column]

        if mask.any():
            comparison_df[f"{column}_raw"] = raw_df.loc[mask, column]
            comparison_df[f"{column}_db"] = db_df.loc[mask, column]

    return comparison_df, len(raw_df), len(db_df)


def run_data_integrity_test(raw_file_path, start_date, end_date):
    if not os.path.exists(raw_file_path):
        logger.error(f"제공된 raw 파일 경로가 존재하지 않습니다: {raw_file_path}")
        return

    # Raw 데이터 로드
    raw_df = load_raw_data(raw_file_path)
    if raw_df is None:
        logger.error("Raw 데이터 로드 실패")
        return

    # DB 데이터 로드
    try:
        db_df = get_db_data()
    except Exception as e:
        logger.error(f"DB 데이터 로드 실패: {str(e)}")
        return

    # 데이터 비교
    comparison_df, raw_count, db_count = compare_data(
        raw_df, db_df, start_date, end_date
    )

    if raw_count == 0 and db_count == 0:
        logger.warning("선택한 날짜 범위에 해당하는 데이터가 없습니다.")
        return

    # 결과 분석
    mismatched_rows = len(comparison_df)

    logger.info(f"Raw 데이터 레코드 수: {raw_count}")
    logger.info(f"DB 데이터 레코드 수: {db_count}")
    logger.info(f"불일치 레코드 수: {mismatched_rows}")

    if raw_count > 0:
        logger.info(f"데이터 불일치율: {mismatched_rows/raw_count:.2%}")

    if mismatched_rows > 0:
        logger.info("불일치하는 데이터 샘플:")
        logger.info(comparison_df.head())

    return comparison_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="데이터 무결성 테스트")
    parser.add_argument(
        "--raw",
        type=str,
        default="C:\\MyMain\\Teckwah\\download\\xlsx_files_complete\\200706_200712_ReceivingTAT_report.xlsx",
    )
    parser.add_argument("--start_date", type=str, default="2024-07-06")
    parser.add_argument("--end_date", type=str, default="2024-07-12")
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if start_date > end_date:
            raise ValueError("시작 날짜가 종료 날짜보다 늦습니다.")
    except ValueError as e:
        logger.error(f"날짜 형식 오류: {str(e)}")
        exit(1)

    run_data_integrity_test(args.raw, start_date, end_date)
