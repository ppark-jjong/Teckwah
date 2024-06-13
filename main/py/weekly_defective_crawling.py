import sys
import time
import datetime
import os
import pyautogui as pg
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as WB
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
import main.py.login_crawling as login_crawling

# 명령줄 인수로부터 날짜 및 로그인 정보를 받아옵니다.
username, password = sys.argv[1:]

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"

# 드라이버 초기화 및 로그인
driver = login_crawling.initialize_driver(download_folder)
login_crawling.login(driver, username, password)
login_crawling.search_report(driver)


# 지정된 report 찾기 및 다운로드 함수
def process_main(driver):
    try:
        # 지정된 report 찾기 로직
        WB(driver, 30).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.x-grid3-row:nth-child(38)")
            )
        )
        elementName = driver.find_element(
            By.CSS_SELECTOR, "div.x-grid3-row:nth-child(38)"
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

    try:
        element_gen351 = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-gen351"))
        )
        element_gen351.click()
        action.send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

        element_confirm = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-gen339"))
        )
        element_confirm.click()
    except TimeoutException as e:
        error_message = f"4. TimeoutException 발생: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        driver.quit()
        exit(1)
    except NoSuchElementException as e:
        error_message = f"4. NoSuchElementException 발생: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        driver.quit()
        exit(1)
    except WebDriverException as e:
        error_message = f"4. WebDriverException 발생: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        driver.quit()
        exit(1)
    except Exception as e:
        error_message = f"4. An unexpected error 발생 콘솔을 확인하세요: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        driver.quit()
        exit(1)


# 다운로드 대기
def wait_for_download(download_folder, timeout=60):
    start_time = time.time()
    downloaded_file = None
    while True:
        files = [f for f in os.listdir(download_folder) if f.endswith(".xlsx")]
        if files:
            downloaded_file = max(
                [os.path.join(download_folder, f) for f in files], key=os.path.getctime
            )
            break
        if time.time() - start_time > timeout:
            print("다운로드 대기 시간 초과")
            break
        time.sleep(1)
    return downloaded_file


# 다운로드된 파일의 이름을 변경하는 함수
def rename_downloaded_file(downloaded_file, new_name):
    if downloaded_file and os.path.exists(downloaded_file):
        os.rename(downloaded_file, os.path.join(download_folder, new_name))
        print(f"파일 이름이 {new_name}(으)로 변경되었습니다.")
    else:
        print("다운로드된 파일을 찾을 수 없습니다.")


# 지정된 report 찾기 및 다운로드 실행
process_main(driver)

# 파일 다운로드 대기
downloaded_file = wait_for_download(download_folder)

# 오늘 날짜를 얻음
today = datetime.date.today()

# 첫째 주를 기준으로 현재 날짜가 몇 번째 주인지 계산
# 월의 첫째 주는 1일이 포함된 주로 시작
first_day_of_month = today.replace(day=1)
first_day_of_week = first_day_of_month.weekday()  # 월요일을 기준으로 0 ~ 6
days_since_first_day = today.day + first_day_of_week

# 몇 번째 주인지 계산
week_number = (days_since_first_day - 1) // 7 + 1

# 다운로드된 파일의 이름 변경
new_name = f"{today.month}월_{week_number}주_Weekly_Defective_Report.xlsx"
rename_downloaded_file(downloaded_file, new_name)

# 다운로드 완료 알림창
pg.alert(text="다운로드가 완료되었습니다.", title="알림", button="OK")
