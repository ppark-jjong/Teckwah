import logging
import os
import pandas as pd
from datetime import datetime
from config import COMPLETE_FOLDER
from database import get_data_by_inventory_date

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="extract_data.log", filemode="a")
logger = logging.getLogger(__name__)

# 콘솔에 로그 출력을 위한 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def get_user_input() -> tuple:
    """
    사용자로부터 시작 날짜와 종료 날짜를 입력받는 함수
    :return: 시작 날짜와 종료 날짜 튜플
    """
    while True:
        try:
            start_date = input("시작 날짜를 입력하세요 (YYYY-MM-DD): ")
            end_date = input("종료 날짜를 입력하세요 (YYYY-MM-DD): ")
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
            logger.info(f"입력된 날짜: {start_date}부터 {end_date}까지")
            return start_date, end_date
        except ValueError:
            logger.error("잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 다시 입력해주세요.")
            print("잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 다시 입력해주세요.")

def preprocess_extracted_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    추출된 데이터를 전처리하는 함수
    :param df: 원본 데이터프레임
    :return: 전처리된 데이터프레임
    """
    logger.info("추출된 데이터 전처리 시작")
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    date_columns = ['putawaydate', 'inventorydate']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    numeric_columns = ['quantity', 'count_po']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    logger.info("데이터 전처리 완료")
    return df

def analyze_data(df: pd.DataFrame) -> dict:
    """
    데이터를 분석하는 함수
    :param df: 전처리된 데이터프레임
    :return: 주차별 분석과 분야별 분석 결과를 담은 딕셔너리
    """
    logger.info("데이터 분석 시작")
    weekly_analysis = df.groupby(["week", "edi_order_type"]).agg({
        "count_po": "sum",
        "quantity": "sum",
    }).reset_index()
    logger.info("주차별 데이터 분석 완료")

    shipto_analysis = df.groupby(["shiptocode", "edi_order_type"]).agg({
        "count_po": "sum",
        "quantity": "sum",
    }).reset_index()
    logger.info("분야별 데이터 분석 완료")

    return {
        "weekly_analysis": weekly_analysis,
        "shipto_analysis": shipto_analysis
    }

def save_to_excel(data: dict, filename: str):
    """
    분석된 데이터를 엑셀 파일로 저장하는 함수
    :param data: 저장할 데이터 딕셔너리
    :param filename: 저장할 파일 이름
    """
    file_path = os.path.join(COMPLETE_FOLDER, filename)
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        logger.info("전체 데이터를 Excel 파일로 저장 중")
        data["full_data"].to_excel(writer, sheet_name="Full_Data", index=False)

        logger.info("Summary 데이터를 Excel 파일로 저장 중")
        data["summary"].to_excel(writer, sheet_name="Summary", index=False)

        logger.info("주차별 분석 데이터를 Excel 파일로 저장 중")
        data["weekly_analysis"].to_excel(writer, sheet_name="Weekly_Analysis", index=False)

        logger.info("분야별 분석 데이터를 Excel 파일로 저장 중")
        data["shipto_analysis"].to_excel(writer, sheet_name="ShipTo_Analysis", index=False)

    logger.info(f"데이터가 Excel 파일로 저장되었습니다: {file_path}")

def main():
    """
    메인 함수: 전체 프로세스를 실행
    """
    logger.info("프로그램 시작")
    
    start_date, end_date = get_user_input()

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    logger.info(f"날짜 변환 완료: {start_date}부터 {end_date}까지")

    logger.info(f"데이터베이스에서 {start_date.date()}부터 {end_date.date()}까지의 데이터를 추출 중")
    df = get_data_by_inventory_date(start_date, end_date)
    logger.info(f"데이터베이스에서 {len(df)}개의 데이터를 추출 완료")

    if not df.empty:
        df = preprocess_extracted_data(df)
        
        logger.info("데이터 분석 시작")
        analysis_results = analyze_data(df)
        logger.info("데이터 분석 완료")

        filename = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}_ReceivingTAT_analysis.xlsx"

        summary_template_path = "C:/MyMain/Teckwah/download/sample.xlsx"
        logger.info(f"Summary 템플릿 파일을 {summary_template_path}에서 읽는 중")
        summary_df = pd.read_excel(summary_template_path, sheet_name="Summary")
        logger.info("Summary 템플릿 파일 읽기 완료")

        data_to_save = {
            "full_data": df, 
            "summary": summary_df,
            "weekly_analysis": analysis_results["weekly_analysis"],
            "shipto_analysis": analysis_results["shipto_analysis"]
        }

        logger.info("Excel 파일 생성 시작")
        save_to_excel(data_to_save, filename)
        logger.info("Excel 파일 생성 완료")

    else:
        logger.info("지정된 날짜 범위에 해당하는 데이터가 없습니다")
    
    logger.info("프로그램 종료")

if __name__ == "__main__":
    main()