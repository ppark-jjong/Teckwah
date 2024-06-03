import pandas as pd
import sys
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
import time
import os

# 명령줄 인수로부터 날짜 및 로그인 정보를 받아옵니다.
username, password, date_1, date_2, date_3, date_4 = sys.argv[1:7]

# 다운로드 폴더 경로 설정
download_folder = "C:\\Users\\Administrator\\Data\\download\\xlsx_files"

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
element_a = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-comp-1045")))
element_a.send_keys(date_1)

element_b = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-comp-1046")))
element_b.send_keys(date_2)

element_c = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-comp-1047")))
element_c.send_keys(date_3)

element_d = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-comp-1048")))
element_d.send_keys(date_4)

element_gen391 = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-gen391")))
element_gen391.click()
action.send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

element_confirm = WB(driver, 20).until(EC.presence_of_element_located((By.ID, "ext-gen339")))
element_confirm.click()

# 다운로드가 완료될 때까지 대기
time.sleep(30)

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
print("다운로드가 완료되었습니다.")
