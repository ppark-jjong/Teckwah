import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
import logging
from typing import Dict, Any
from config import WEBDRIVER_TIMEOUT, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)

class WebCrawler:
    def __init__(self, config: Dict[str, Any]):
        """
        WebCrawler 클래스를 초기화합니다.
        """
        self.config = config
        self.driver = self._initialize_driver()

    def _initialize_driver(self) -> webdriver.Chrome:
        """
        웹 드라이버를 초기화합니다.
        """
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": self.config["DOWNLOAD_FOLDER"],
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        return webdriver.Chrome(options=chrome_options)

    def login(self, username: str, password: str) -> None:
        """
        웹사이트에 로그인합니다.
        """
        try:
            self._perform_login(username, password)
        except (TimeoutException, NoSuchElementException, WebDriverException) as e:
            logger.error(f"로그인 실패: {str(e)}")
            raise

    def _perform_login(self, username: str, password: str) -> None:
        """
        실제 로그인 작업을 수행합니다.
        """
        self.driver.get("https://cs.vinfiniti.biz:8227/")
        WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.ID, "userName"))
        ).send_keys(username)
        WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.NAME, "password"))
        ).send_keys(password)
        WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.NAME, "project"))
        ).send_keys("cs")
        WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))
        ).click()

    def search_report(self) -> None:
        """
        리포트를 검색합니다.
        """
        self._retry_action(self._perform_search_report)

    def _perform_search_report(self) -> None:
        """
        실제 리포트 검색 작업을 수행합니다.
        """
        WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.ID, "ext-comp-1003"))
        )
        time.sleep(5)
        element = self.driver.find_element(By.ID, "ext-comp-1003")
        time.sleep(3)
        element.send_keys("repo")
        time.sleep(2)
        ActionChains(self.driver).send_keys("r").perform()
        element.send_keys(Keys.RETURN)

    def process_rma_return(self, start_date: str, end_date: str) -> None:
        """
        RMA 반환 프로세스를 수행합니다.
        """
        try:
            self._perform_rma_return(start_date, end_date)
        except (TimeoutException, NoSuchElementException, WebDriverException) as e:
            logger.error(f"RMA 반환 처리 실패: {str(e)}")
            raise

    def _perform_rma_return(self, start_date: str, end_date: str) -> None:
        """
        실제 RMA 반환 프로세스를 수행합니다.
        """
        try:
            WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div:nth-child(28)")
                )
            )
            elementName = self.driver.find_element(
                By.CSS_SELECTOR, "div:nth-child(28)"
            )
            elementName.click()
            action = ActionChains(self.driver)
            action.context_click(elementName).perform()
            time.sleep(1)
            action.send_keys(Keys.DOWN).send_keys(Keys.ENTER).perform()

            element_a = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "ext-comp-1045"))
            )
            element_a.send_keys(start_date)

            element_b = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "ext-comp-1046"))
            )
            element_b.send_keys(end_date)

            element_gen390 = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "ext-gen371"))
            )
            element_gen390.click()
            action.send_keys(Keys.ENTER).perform()

            element_gen403 = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "ext-gen384"))
            )
            element_gen403.click()
            action.send_keys(Keys.DOWN).send_keys(Keys.DOWN).send_keys(
                Keys.ENTER
            ).perform()

            element_confirm = WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "ext-gen339"))
            )
            element_confirm.click()
        except (TimeoutException, NoSuchElementException, WebDriverException) as e:
            logger.error(f"RMA 반환 처리 실패: {e.__class__.__name__} - {str(e)}")
            raise

    def _retry_action(self, action, *args):
        """
        지정된 횟수만큼 작업을 재시도합니다.
        """
        for attempt in range(MAX_RETRIES):
            try:
                return action(*args)
            except (TimeoutException, NoSuchElementException, WebDriverException) as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"All {MAX_RETRIES} attempts failed")
                    raise
                time.sleep(RETRY_DELAY)

    def close(self) -> None:
        """
        웹 드라이버를 종료합니다.
        """
        self.driver.quit()


def initialize_and_login(
    config: Dict[str, Any], username: str, password: str
) -> WebCrawler:
    """
    WebCrawler를 초기화하고 로그인합니다.
    """
    crawler = WebCrawler(config)
    try:
        crawler.login(username, password)
        crawler.search_report()
        return crawler
    except Exception as e:
        logger.error(f"초기화 및 로그인 실패: {str(e)}")
        crawler.close()
        raise
