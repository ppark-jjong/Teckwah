import logging
import os
import pandas as pd
from datetime import datetime
from database import get_data_by_fy_and_quarter
from config import COMPLETE_FOLDER

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="extract_data.log", filemode="a")
logger = logging.getLogger(__name__)

def get_user_input() -> tuple:
    """사용자로부터 FY와 Quarter 입력 받기"""
    fy = input("Enter FY (e.g., FY23): ")
    quarter = input("Enter Quarter (e.g., Q1, Q2): ")
    return fy, quarter

def analyze_data(df: pd.DataFrame) -> dict:
    """데이터 분석 수행"""
    # WK별 분석
    weekly_analysis = df.groupby('Week').agg({
        'Count_PO': 'sum',  # 오더 개수
        'Quantity': 'sum',  # 오더 수량
    }).reset_index()

    # ShipToCode별 분석
    shipto_analysis = df.groupby('ShipToCode').agg({
        'Count_PO': 'sum',  # 오더 개수
        'Quantity': 'sum',  # 오더 수량
    }).reset_index()

    return {
        'weekly_analysis': weekly_analysis,
        'shipto_analysis': shipto_analysis
    }

def save_to_csv(data: pd.DataFrame, filename: str):
    """데이터를 CSV 파일로 저장"""
    file_path = os.path.join(COMPLETE_FOLDER, filename)
    data.to_csv(file_path, index=False)
    logger.info(f"데이터가 CSV 파일로 저장되었습니다: {file_path}")

def main():
    try:
        fy, quarter = get_user_input()

        logger.info(f"데이터베이스에서 {fy} {quarter} 데이터를 추출합니다.")
        df = get_data_by_fy_and_quarter(fy, quarter)
        logger.info(f"총 {len(df)}개의 데이터가 추출되었습니다.")

        if not df.empty:
            analysis_results = analyze_data(df)

            # 결과 저장
            base_filename = f"{fy}_{quarter}_ReceivingTAT_analysis"

            # 전체 데이터 저장
            save_to_csv(df, f"{base_filename}_full_data.csv")

            # WK별 분석 결과 저장
            save_to_csv(analysis_results['weekly_analysis'], f"{base_filename}_weekly_analysis.csv")

            # ShipToCode별 분석 결과 저장
            save_to_csv(analysis_results['shipto_analysis'], f"{base_filename}_shipto_analysis.csv")

        else:
            logger.info("지정된 FY와 Quarter에 해당하는 데이터가 없습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()