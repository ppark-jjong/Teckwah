import sys
import time
import os
import pyautogui as pg
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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

def initialize_driver(download_folder):
    chrome_options = Options()
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def login(driver, username, password):
    driver.get("https://cs.vinfiniti.biz:8227/")
    try:
        WB(driver, 10).until(
            EC.presence_of_element_located((By.ID, "userName"))
        ).send_keys(username)
        WB(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "password"))
        ).send_keys(password)
        WB(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "project"))
        ).send_keys("cs")
        WB(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))
        ).click()
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


def search_report(driver):
    try:
        WB(driver, 10).until(EC.presence_of_element_located((By.ID, "ext-comp-1003")))
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
