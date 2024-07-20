import pyautogui as pg
import pandas as pd
from datetime import datetime, timedelta
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER
from web_crawler import initialize_and_login, process_rma_return
from file_handler import (
    get_existing_files,
    wait_for_download,
    rename_downloaded_file,
    process_file,
    format_date_yy_mm_dd,
)
from database import create_tables, get_db_data
from data_processor import process_dataframe

def get_week_dates(start_date, end_date):
    current = start_date
    while current <= end_date:
        week_start = current
        week_end = min(current + timedelta(days=6), end_date)
        yield week_start, week_end
        current = week_end + timedelta(days=1)

def main():
    username = "jhypark-dir"
    password = "Hyeok970209!"
    start_date = datetime(2023, 2, 24)
    end_date = datetime(2024, 7, 5)

    try:
        driver = initialize_and_login(DOWNLOAD_FOLDER, username, password)
        create_tables()

        for week_start, week_end in get_week_dates(start_date, end_date):
            startDate = week_start.strftime("%Y-%m-%d")
            endDate = week_end.strftime("%Y-%m-%d")
            
            print(f"Processing week: {startDate} to {endDate}")

            existing_files = get_existing_files(DOWNLOAD_FOLDER)

            start_date_yy_mm_dd = format_date_yy_mm_dd(startDate)
            end_date_yy_mm_dd = format_date_yy_mm_dd(endDate)
            new_name = f"{start_date_yy_mm_dd}_{end_date_yy_mm_dd}_ReceivingTAT_report.xlsx"

            print("Report를 찾고 다운로드를 시작합니다.")
            process_rma_return(driver, startDate, endDate)

            new_files = wait_for_download(DOWNLOAD_FOLDER, existing_files)

            print("다운로드된 파일의 이름을 변경합니다.")
            file_path = rename_downloaded_file(DOWNLOAD_FOLDER, new_files, new_name)

            if file_path:
                print("다운로드된 파일을 처리합니다.")
                new_file_path = process_file(file_path)

                print(f"주 {startDate} - {endDate} 처리 완료")
            else:
                print(f"주 {startDate} - {endDate} 파일 다운로드 실패")

    except Exception as e:
        error_message = f"프로그램 실행 중 오류 발생: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
    finally:
        if 'driver' in locals():
            driver.quit()
            print("드라이버를 종료합니다.")

    print("모든 데이터 처리가 완료되었습니다.")
    pg.alert(text="모든 주의 데이터 처리가 완료되었습니다.", title="알림", button="OK")

if __name__ == "__main__":
    main()