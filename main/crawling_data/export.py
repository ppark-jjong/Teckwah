import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from database import get_data_by_inventory_date
from config import COMPLETE_FOLDER

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="extract_data.log", filemode="a")
logger = logging.getLogger(__name__)

def get_user_input() -> tuple:
    """사용자로부터 시작 날짜와 종료 날짜 입력 받기"""
    try:
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        return start_date, end_date
    except Exception as e:
        logger.error(f"Error in get_user_input: {str(e)}")
        raise

def get_dell_week(date: datetime) -> str:
    """주어진 날짜에 해당하는 Dell의 Week 값을 반환"""
    try:
        year = date.year if date.month >= 2 else date.year - 1
        fy_start = datetime(year, 2, 1) + timedelta(days=(5 - datetime(year, 2, 1).weekday() + 7) % 7)
        week_number = ((date - fy_start).days // 7) + 1
        return f"WK{week_number:02d}"
    except Exception as e:
        logger.error(f"Error in get_dell_week: {str(e)}")
        raise

def analyze_data(df: pd.DataFrame) -> dict:
    """데이터 분석 수행"""
    try:
        weekly_analysis = df.groupby('Week').agg({
            'Count_PO': 'sum',
            'Quantity': 'sum',
        }).reset_index()

        shipto_analysis = df.groupby('ShipToCode').agg({
            'Count_PO': 'sum',
            'Quantity': 'sum',
        }).reset_index()

        return {
            'weekly_analysis': weekly_analysis,
            'shipto_analysis': shipto_analysis
        }
    except Exception as e:
        logger.error(f"Error in analyze_data: {str(e)}")
        raise

def update_summary_sheet(summary_df, weekly_analysis, shipto_analysis):
    try:
        logger.info("Updating Summary sheet with weekly analysis data.")
        
        # 주차별 합계 데이터 업데이트
        for index, row in weekly_analysis.iterrows():
            week = row['Week']
            count_po = row['Count_PO']
            
            # 주차별 데이터 업데이트
            summary_df.loc[summary_df['CountPO'] == week, 'DIRSEL'] = count_po  # DIRSEL 부분 예시
            # 다른 분야들도 동일하게 업데이트
            
            # 합계 업데이트
            summary_df.loc[summary_df['CountPO'] == week, 'Total'] = summary_df.loc[summary_df['CountPO'] == week, ['DIRSEL', 'DIRTAJ', 'DIRPUS', 'DIRKWJ']].sum(axis=1)
        
        logger.info("Updating Summary sheet with ship-to analysis data.")
        
        # 분야별 합계 데이터 업데이트
        for index, row in shipto_analysis.iterrows():
            shipto_code = row['ShipToCode']
            count_po = row['Count_PO']
            
            # 분야별 데이터 업데이트
            summary_df.loc[summary_df['ShipToCode'] == shipto_code, 'DIRSEL'] = count_po  # DIRSEL 부분 예시
            # 다른 분야들도 동일하게 업데이트
            
            # 합계 업데이트
            summary_df.loc[summary_df['ShipToCode'] == shipto_code, 'Total'] = summary_df.loc[summary_df['ShipToCode'] == shipto_code, ['DIRSEL', 'DIRTAJ', 'DIRPUS', 'DIRKWJ']].sum(axis=1)
        
        logger.info("Updating Full_Total in Summary sheet.")
        
        # Full_Total 업데이트
        summary_df['Full_Total'] = summary_df['Total'].sum()
        
        return summary_df
    except Exception as e:
        logger.error(f"Error in update_summary_sheet: {str(e)}")
        raise

def save_to_excel(data: dict, filename: str):
    """데이터를 Excel 파일로 저장"""
    try:
        file_path = os.path.join(COMPLETE_FOLDER, filename)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            logger.info("Saving full data to Excel.")
            data['full_data'].to_excel(writer, sheet_name='Full_Data', index=False)
            
            logger.info("Saving summary data to Excel.")
            data['summary'].to_excel(writer, sheet_name='Summary', index=False)
            
        logger.info(f"Data saved to Excel file: {file_path}")
    except Exception as e:
        logger.error(f"Error in save_to_excel: {str(e)}")
        raise

def main():
    try:
        start_date, end_date = get_user_input()
        logger.info(f"Received input dates: {start_date} to {end_date}")

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        logger.info(f"Fetching data from {start_date.date()} to {end_date.date()} from the database.")
        df = get_data_by_inventory_date(start_date, end_date)
        logger.info(f"Fetched {len(df)} records from the database.")

        if not df.empty:
            analysis_results = analyze_data(df)
            logger.info("Data analysis complete.")

            start_week = get_dell_week(start_date)
            end_week = get_dell_week(end_date)
            
            filename = f"{start_week}-{end_week}_ReceivingTAT_analysis.xlsx"

            summary_template_path = 'C:\\MyMain\\Teckwah\\download\\sample.xlsx'  # 예시 엑셀 파일 경로
            logger.info(f"Reading Summary template from {summary_template_path}")
            summary_df = pd.read_excel(summary_template_path, sheet_name='Summary')
            logger.info("Read Summary template successfully.")

            updated_summary_df = update_summary_sheet(summary_df, analysis_results['weekly_analysis'], analysis_results['shipto_analysis'])

            data_to_save = {
                'full_data': df,
                'summary': updated_summary_df
            }

            save_to_excel(data_to_save, filename)
            logger.info("Excel file generation complete.")

        else:
            logger.info("No data found for the specified date range.")

    except ValueError as ve:
        logger.error(f"Date input error: {str(ve)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
