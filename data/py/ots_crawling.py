import sys
import time
import os
import pyautogui as pg
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as WB
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import login_crawling

# 명령줄 인수로부터 날짜 및 로그인 정보를 받아옵니다.
username, password, date_1, date_2, date_3, date_4 = sys.argv[1:]

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"

# 드라이버 초기화 및 로그인
driver = login_crawling.initialize_driver(download_folder)
login_crawling.login(driver, username, password)
login_crawling.search_report(driver)

# 기존 파일 목록을 가져오는 함수
def get_existing_files(download_folder):
    return set(os.listdir(download_folder))

# 지정된 report 찾기 및 다운로드 함수
def process_ots(driver, date_1, date_2, date_3, date_4):
    try:
        # 지정된 report 찾기 로직
        WB(driver, 30).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "div.x-grid3-row:nth-child(34)"
                )
            )
        )
        elementName = driver.find_element(
            By.CSS_SELECTOR,
            "div.x-grid3-row:nth-child(34)"
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
        # 원하는 날짜나 기준 입력 로직
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

# 다운로드된 파일을 기다리는 함수
def wait_for_download(download_folder, existing_files, timeout=60):
    start_time = time.time()
    while True:
        current_files = set(os.listdir(download_folder))
        new_files = current_files - existing_files
        if any(file.endswith(".xlsx") for file in new_files):
            return new_files
        if time.time() - start_time > timeout:
            print("다운로드 대기 시간 초과")
            break
        time.sleep(1)
    return set()

# 다운로드된 파일의 이름을 변경하는 함수
def rename_downloaded_file(download_folder, new_files, new_name):
    if new_files:
        latest_file = max(
            [os.path.join(download_folder, f) for f in new_files if f.endswith(".xlsx")], key=os.path.getctime
        )
        os.rename(latest_file, os.path.join(download_folder, new_name))
        success_message = f"파일 이름이 {new_name}(으)로 변경되었습니다."
        print(success_message)
        pg.alert(text=success_message, title="알림", button="OK")
    else:
        error_message = "다운로드된 파일을 찾을 수 없습니다."
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")

# 메인 실행 부분
if __name__ == "__main__":
    try:
        # 기존 파일 목록을 저장합니다.
        existing_files = get_existing_files(download_folder)

        # 바꿀 파일 이름 지정
        new_name = f"{date_1}_{date_2}_OTS.xlsx"

        # 지정된 report 찾기 및 다운로드 실행
        process_ots(driver, date_1, date_2, date_3, date_4)

        # 파일 다운로드 대기
        new_files = wait_for_download(download_folder, existing_files)

        # 다운로드된 파일의 이름 변경
        rename_downloaded_file(download_folder, new_files, new_name)

        # 다운로드 완료 알림창
        pg.alert(text="다운로드가 완료되었습니다.", title="알림", button="OK")
    finally:
        # 드라이버 종료
        driver.quit()
