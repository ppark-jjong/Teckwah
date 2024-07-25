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

def save_to_excel(data: dict, filename: str):
    """데이터를 Excel 파일로 저장"""
    file_path = os.path.join(COMPLETE_FOLDER, filename)
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        data['full_data'].to_excel(writer, sheet_name='Full Data', index=False)
        data['weekly_analysis'].to_excel(writer, sheet_name='Weekly Analysis', index=False)
        data['shipto_analysis'].to_excel(writer, sheet_name='ShipTo Analysis', index=False)
    logger.info(f"데이터가 Excel 파일로 저장되었습니다: {file_path}")

def main():
    try:
        fy, quarter = get_user_input()

        logger.info(f"데이터베이스에서 {fy} {quarter} 데이터를 추출합니다.")
        df = get_data_by_fy_and_quarter(fy, quarter)
        logger.info(f"총 {len(df)}개의 데이터가 추출되었습니다.")

        if not df.empty:
            analysis_results = analyze_data(df)

            # 결과 저장
            filename = f"{fy}_{quarter}_ReceivingTAT_analysis.xlsx"

            data_to_save = {
                'full_data': df,
                'weekly_analysis': analysis_results['weekly_analysis'],
                'shipto_analysis': analysis_results['shipto_analysis']
            }

            save_to_excel(data_to_save, filename)
            logger.info(f"분석 결과가 {filename}에 저장되었습니다.")

        else:
            logger.info("지정된 FY와 Quarter에 해당하는 데이터가 없습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()