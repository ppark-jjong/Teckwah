import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as WB
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pyautogui as pg
import login_crawling

def initialize_and_login(download_folder, username, password):
    try:
        driver = login_crawling.initialize_driver(download_folder)
        login_crawling.login(driver, username, password)
        login_crawling.search_report(driver)
        return driver
    except Exception as e:
        error_message = f"초기화 및 로그인 실패: {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        raise

def process_rma_return(driver, start_date, end_date):
    try:
        WB(driver, 30).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.x-grid3-row:nth-child(28)")
            )
        )
        elementName = driver.find_element(
            By.CSS_SELECTOR, "div.x-grid3-row:nth-child(28)"
        )
        elementName.click()
        action = ActionChains(driver)
        action.context_click(elementName).perform()
        time.sleep(1)
        action.send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

        element_a = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-comp-1045"))
        )
        element_a.send_keys(start_date)

        element_b = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-comp-1046"))
        )
        element_b.send_keys(end_date)

        element_gen390 = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-gen371"))
        )
        element_gen390.click()
        action.send_keys(Keys.ENTER).perform()

        element_gen403 = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-gen384"))
        )
        element_gen403.click()
        action.send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

        element_confirm = WB(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ext-gen339"))
        )
        element_confirm.click()
    except (TimeoutException, NoSuchElementException, WebDriverException) as e:
        error_message = f"RMA 반환 처리 실패: {e.__class__.__name__} - {str(e)}"
        print(error_message)
        pg.alert(text=error_message, title="Error", button="OK")
        raise