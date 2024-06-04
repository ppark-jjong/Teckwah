import os
import sys
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as WB
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
    TimeoutException,
)
import pyautogui as pg
import mysql.connector
from mysql.connector import Error

# 명령줄 인수로부터 날짜 및 로그인 정보를 받아옵니다.
username, password, date_1, date_2, date_3, date_4 = sys.argv[1:]
# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"
# 변환 완료된 파일을 저장할 폴더 경로 설정
xlsx_complete_folder = os.path.join("C:\\MyMain\\Teckwah\\download\\", "xlsx_files_complete")

# 크롬 옵션 설정
chrome_options = Options()
prefs = {
    "download.default_directory": download_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}
chrome_options.add_experimental_option("prefs", prefs)

# 웹 드라이버 초기화
driver = webdriver.Chrome(service=Service(), options=chrome_options)

# 웹 페이지 열기
driver.get("https://cs.vinfiniti.biz:8227/")

# 로그인 프로세스
try:
    WB(driver, 10).until(EC.presence_of_element_located((By.ID, "userName"))).send_keys(username)
    WB(driver, 10).until(EC.presence_of_element_located((By.NAME, "password"))).send_keys(password)
    WB(driver, 10).until(EC.presence_of_element_located((By.NAME, "project"))).send_keys("cs")
    WB(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))).click()
    print("로그인 성공")
    pg.alert(text="로그인 성공", title="알림", button="OK")
except NoSuchElementException as e:
    error_message = f"1. NoSuchElementException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except WebDriverException as e:
    error_message = f"1. WebDriverException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except Exception as e:
    error_message = f"1. An unexpected error 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)

# report 검색 로직
try:
    time.sleep(5)
    element = driver.find_element(By.ID, "ext-comp-1003")
    time.sleep(3)
    element.send_keys("repo")
    time.sleep(2)
    pg.typewrite("r")
    element.send_keys(Keys.RETURN)
    print("리포트 검색 성공")
    pg.alert(text="리포트 검색 성공", title="알림", button="OK")
except NoSuchElementException as e:
    error_message = f"2. NoSuchElementException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except WebDriverException as e:
    error_message = f"2. WebDriverException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except Exception as e:
    error_message = f"2. An unexpected error 발생 콘솔을 확인하세요: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)

# 지정된 report 찾기 로직
try:
    WB(driver, 30).until(
        EC.presence_of_element_located(
            (
                By.CSS_SELECTOR,
                "div.x-grid3-row:nth-child(34) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > div:nth-child(1)",
            )
        )
    )
    elementName = driver.find_element(
        By.CSS_SELECTOR,
        "div.x-grid3-row:nth-child(34) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > div:nth-child(1)",
    )
    elementName.click()
    action = ActionChains(driver)
    action.context_click(elementName).perform()
    time.sleep(1)  # 메뉴가 나타날 시간 대기
    action.send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()
    print("리포트 찾기 및 열기 성공")
    pg.alert(text="리포트 찾기 및 열기 성공", title="알림", button="OK")
except TimeoutException as e:
    error_message = f"3. TimeoutException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except NoSuchElementException as e:
    error_message = f"3. NoSuchElementException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except WebDriverException as e:
    error_message = f"3. WebDriverException 발생: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)
except Exception as e:
    error_message = f"3. An unexpected error 발생 콘솔을 확인하세요: {str(e)}"
    print(error_message)
    pg.alert(text=error_message, title="Error", button="OK")
    driver.quit()
    exit(1)

# 원하는 날짜나 기준 입력 로직 (client 요청으로 교체 예정)
element_a = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-comp-1045"))
)
element_a.send_keys(date_1)

element_b = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-comp-1046"))
)
element_b.send_keys(date_2)

element_c = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-comp-1047"))
)
element_c.send_keys(date_3)

element_d = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-comp-1048"))
)
element_d.send_keys(date_4)

element_gen391 = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-gen391"))
)
element_gen391.click()
action.send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

element_confirm = WB(driver, 20).until(
    EC.presence_of_element_located((By.ID, "ext-gen339"))
)
element_confirm.click()

# 다운로드가 완료될 때까지 대기
time.sleep(30)
print("다운로드 완료")

# 다운로드된 파일의 이름을 변경하는 함수
def rename_downloaded_file(download_folder, new_name):
    files = os.listdir(download_folder)
    files = [f for f in files if f.endswith(".xlsx")]  # 엑셀 파일 필터링

    if files:
        latest_file = max(
            [os.path.join(download_folder, f) for f in files], key=os.path.getctime
        )
        os.rename(latest_file, os.path.join(download_folder, new_name))
        print(f"파일 이름이 {new_name}(으)로 변경되었습니다.")
    else:
        print("다운로드된 파일을 찾을 수 없습니다.")

# 다운로드된 파일의 이름 변경
new_name = f"OTS_{date_1}_{date_2}.xlsx"
rename_downloaded_file(download_folder, new_name)

# 다운로드 완료 알림창
pg.alert(text="다운로드가 완료되었습니다.", title="알림", button="OK")

# MySQL 연결 설정
mysql_connection_params = {
    "host": "localhost",
    "database": "teckwah_test",
    "user": "root",
    "password": "1234",
    "allow_local_infile": True,  # LOCAL INFILE 옵션 활성화
}

def sanitize_column_names(df):
    """
    열 이름을 정리하는 함수. 공백을 밑줄로 대체하고 중복 열 이름을 고유하게 변경합니다.
    """
    df.columns = [col.strip().replace(' ', '_') for col in df.columns]
    seen = {}
    new_columns = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)
    df.columns = new_columns
    return df

def create_table_from_df(cursor, table_name, df):
    """
    DataFrame으로부터 MySQL 테이블을 생성하는 함수
    """
    columns_with_types = ", ".join([f"`{col}` TEXT" for col in df.columns])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_with_types}
    );
    """
    cursor.execute(create_table_sql)

def insert_data_from_df(cursor, table_name, df):
    """
    DataFrame 데이터를 MySQL 테이블에 삽입하는 함수
    """
    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_sql = f"""
        INSERT INTO `{table_name}` ({columns})
        VALUES ({placeholders});
        """
        cursor.execute(insert_sql, tuple(str(value) if pd.notna(value) else None for value in row))

def process_xlsx_file(xlsx_file, mysql_connection_params):
    """
    엑셀 파일을 처리하여 MySQL 테이블로 변환하는 함수
    """
    xlsx_path = os.path.join(download_folder, xlsx_file)
    table_name = os.path.splitext(xlsx_file)[0]

    try:
        # 엑셀 파일을 읽어서 DataFrame으로 변환
        df = pd.read_excel(xlsx_path)

        # 모든 값이 NaN인 열을 제거
        df.dropna(axis=1, how='all', inplace=True)

        # 'From Country' 열 삭제 (정확히 일치하는 열만 삭제)
        if 'From Country' in df.columns:
            df.drop(columns=['From Country'], inplace=True)

        # 열 이름을 정리 (중복 제거 및 공백을 밑줄로 대체)
        df = sanitize_column_names(df)

        # MySQL 데이터베이스에 연결
        connection = mysql.connector.connect(**mysql_connection_params)
        cursor = connection.cursor()

        # 테이블이 존재하지 않는 경우 생성
        create_table_from_df(cursor, table_name, df)

        # 데이터 삽입
        insert_data_from_df(cursor, table_name, df)

        # 커밋
        connection.commit()

        # 변환된 .xlsx 파일을 xlsx_files_complete 폴더로 이동
        complete_path = os.path.join(xlsx_complete_folder, xlsx_file)
        os.rename(xlsx_path, complete_path)

        print(f"파일 변환 완료: {xlsx_file}를 {table_name} 테이블로 변환하고 {xlsx_complete_folder}로 이동했습니다.")
    except Exception as e:
        print(f"파일 변환 중 오류 발생: {xlsx_file}, 오류: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 모든 .xlsx 파일을 처리
pg.alert(text="MySQL 변환 시작", title="알림", button="OK")
print("MySQL 변환 시작")
for xlsx_file in os.listdir(download_folder):
    if xlsx_file.endswith(".xlsx") and not xlsx_file.startswith("~$"):
        process_xlsx_file(xlsx_file, mysql_connection_params)

print("모든 .xlsx 파일을 MySQL로 변환했습니다.")
pg.alert(text="모든 .xlsx 파일을 MySQL로 변환했습니다.", title="알림", button="OK")
