import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG, COMPLETE_FOLDER

# 로깅 설정
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", filename="export_data.log", filemode="a")
logger = logging.getLogger(__name__)

def get_user_input() -> tuple:
    """사용자로부터 FY와 Quarter 입력 받기"""
    fy = input("Enter FY (e.g., FY23): ")
    quarter = input("Enter Quarter (e.g., Q1, Q2): ")
    return fy, quarter

def get_data_from_db(fy: str, quarter: str) -> pd.DataFrame:
    """데이터베이스에서 데이터 추출"""
    connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)

    query = text("""
    SELECT * FROM receiving_tat_report
    WHERE FY = :fy AND Quarter = :quarter
    """)

    try:
        with engine.connect() as connection:
            logger.debug(f"Executing query: {query}")
            logger.debug(f"Query parameters: fy={fy}, quarter={quarter}")
            df = pd.read_sql_query(query, connection, params={"fy": fy, "quarter": quarter})
        logger.info(f"데이터베이스에서 {len(df)}개의 레코드를 추출했습니다.")
        logger.debug(f"Data sample: \n{df.head()}")
        return df
    except Exception as e:
        logger.error(f"데이터베이스에서 데이터를 가져오는 중 오류 발생: {str(e)}", exc_info=True)
        return pd.DataFrame()

def analyze_data(df: pd.DataFrame) -> dict:
    """데이터 분석 수행"""
    if df.empty:
        logger.warning("분석할 데이터가 없습니다.")
        return {}

    # WK별 분석
    weekly_analysis = df.groupby('Week').agg({
        'Count_PO': 'sum',
        'Quantity': 'sum',
    }).reset_index()

    # ShipToCode별 분석
    shipto_analysis = df.groupby('ShipToCode').agg({
        'Count_PO': 'sum',
        'Quantity': 'sum',
    }).reset_index()

    logger.debug(f"Weekly analysis sample: \n{weekly_analysis.head()}")
    logger.debug(f"ShipTo analysis sample: \n{shipto_analysis.head()}")

    return {
        'weekly_analysis': weekly_analysis,
        'shipto_analysis': shipto_analysis
    }

def save_to_excel(data: dict, filename: str):
    """데이터를 Excel 파일로 저장"""
    if not os.path.exists(COMPLETE_FOLDER):
        os.makedirs(COMPLETE_FOLDER)
    file_path = os.path.join(COMPLETE_FOLDER, filename)
    
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        data['full_data'].to_excel(writer, sheet_name='Full Data', index=False)
        data['weekly_analysis'].to_excel(writer, sheet_name='Weekly Analysis', index=False)
        data['shipto_analysis'].to_excel(writer, sheet_name='ShipTo Analysis', index=False)
    
    logger.info(f"데이터가 Excel 파일로 저장되었습니다: {file_path}")

def main():
    try:
        fy, quarter = get_user_input()
        logger.info(f"{fy} {quarter} 데이터 추출 및 분석을 시작합니다.")

        df = get_data_from_db(fy, quarter)
        if df.empty:
            logger.warning("추출된 데이터가 없습니다. 프로그램을 종료합니다.")
            return

        analysis_results = analyze_data(df)
        if not analysis_results:
            logger.warning("분석 결과가 없습니다. 프로그램을 종료합니다.")
            return

        # 결과를 하나의 딕셔너리로 합치기
        data_to_save = {
            'full_data': df,
            'weekly_analysis': analysis_results['weekly_analysis'],
            'shipto_analysis': analysis_results['shipto_analysis']
        }

        # Excel 파일로 저장
        filename = f"{fy}_{quarter}_ReceivingTAT_analysis.xlsx"
        save_to_excel(data_to_save, filename)

        logger.info("모든 데이터 처리 및 저장이 완료되었습니다.")

    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()