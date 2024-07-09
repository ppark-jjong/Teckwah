import os
import pyautogui as pg
import pandas as pd
import numpy as np
import mysql.connector
from selenium.common.exceptions import WebDriverException
from config import DOWNLOAD_FOLDER, COMPLETE_FOLDER, DB_CONFIG
from web_crawler import initialize_and_login, process_rma_return
from file_handler import get_existing_files, wait_for_download, rename_downloaded_file, process_file, format_date_yy_mm_dd
from database import create_tables, get_db_data
from data_processor import process_dataframe

def compare_dataframes(df1, df2, tolerance=1e-5):
    if df1.shape != df2.shape:
        return False
    for column in df1.columns:
        if df1[column].dtype in ['float64', 'float32']:
            if not np.allclose(df1[column], df2[column], atol=tolerance, equal_nan=True):
                return False
        elif not df1[column].equals(df2[column]):
            return False
    return True

def main():
    driver = None
    try:
        username = 'jhypark-dir'
        password = 'Hyeok970209!'
        startDate = input("Enter start date (YYYY-MM-DD): ")
        endDate = input("Enter end date (YYYY-MM-DD): ")

        # 웹 크롤러 초기화 및 로그인
        driver = initialize_and_login(DOWNLOAD_FOLDER, username, password)

        # 기존 파일 목록 저장
        existing_files = get_existing_files(DOWNLOAD_FOLDER)

        # 날짜를 YYMMDD 형식으로 변환
        start_date_yy_mm_dd = format_date_yy_mm_dd(startDate)
        end_date_yy_mm_dd = format_date_yy_mm_dd(endDate)

        # 새 파일 이름 지정
        new_name = f"{start_date_yy_mm_dd}_{end_date_yy_mm_dd}_ReceivingTAT_report.xlsx"

        # 지정된 report 찾기 및 다운로드 실행
        print("Report를 찾고 다운로드를 시작합니다.")
        process_rma_return(driver, startDate, endDate)

        # 파일 다운로드 대기
        new_files = wait_for_download(DOWNLOAD_FOLDER, existing_files)

        # 다운로드된 파일의 이름 변경
        print("다운로드된 파일의 이름을 변경합니다.")
        file_path = rename_downloaded_file(DOWNLOAD_FOLDER, new_files, new_name)

        if file_path:
            # 테이블 생성
            create_tables()

            # 다운로드된 파일 처리
            print("다운로드된 파일을 처리합니다.")
            new_file_path = process_file(file_path)

            # 다운로드 완료 알림창
            pg.alert(text="다운로드 및 데이터 처리가 완료되었습니다.", title="알림", button="OK")

            # 테스트 및 결과 검증
            print("테스트를 통해 변환 결과를 검증합니다.")
            df_db = get_db_data()
            df_excel = pd.read_excel(new_file_path, sheet_name="CS Receiving TAT")
            df_excel_processed = process_dataframe(df_excel)

        else:
            print("파일 다운로드에 실패했습니다.")

    except FileNotFoundError as e:
        print(f"파일을 찾을 수 없습니다: {e}")
    except mysql.connector.Error as e:
        print(f"데이터베이스 오류: {e}")
    except WebDriverException as e:
        print(f"웹 드라이버 오류: {e}")
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
    finally:
        if driver:
            driver.quit()
            print("드라이버를 종료합니다.")

if __name__ == "__main__":
    main()