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
import crawling_login

# 명령줄 인수로부터 날짜 및 로그인 정보를 받아옵니다.
username, password, date_1, date_2, date_3, date_4 = sys.argv[1:]

# 다운로드 폴더 경로 설정
download_folder = "C:\\MyMain\\Teckwah\\download\\xlsx_files"

# 드라이버 초기화 및 로그인
driver = crawling_login.initialize_driver(download_folder)
crawling_login.login(driver, username, password)
crawling_login.search_report(driver)

# 지정된 report 찾기 및 다운로드 함수
def process_OTS(driver, date_1, date_2, date_3, date_4):
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

# 지정된 report 찾기 및 다운로드 실행
process_OTS(driver, date_1, date_2, date_3, date_4)

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

# 다운로드 대기
def wait_for_download(download_folder, timeout=60):
    start_time = time.time()
    while True:
        if any(file.endswith(".xlsx") for file in os.listdir(download_folder)):
            break
        if time.time() - start_time > timeout:
            print("다운로드 대기 시간 초과")
            break
        time.sleep(1)

# 파일 다운로드 대기
wait_for_download(download_folder)

# 다운로드된 파일의 이름 변경
new_name = f"OTS_{date_1}_{date_2}.xlsx"
rename_downloaded_file(download_folder, new_name)

# 다운로드 완료 알림창
pg.alert(text="다운로드가 완료되었습니다.", title="알림", button="OK")