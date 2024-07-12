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
        self.config = config
        self.driver = self._initialize_driver()

    def _initialize_driver(self) -> webdriver.Chrome:
        """웹 드라이버를 초기화합니다."""
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

        :param username: 사용자 이름
        :param password: 비밀번호
        """
        self._retry_action(self._perform_login, username, password)

    def _perform_login(self, username: str, password: str) -> None:
        """실제 로그인 작업을 수행합니다."""
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
        """리포트를 검색합니다."""
        self._retry_action(self._perform_search_report)

    def _perform_search_report(self) -> None:
        """실제 리포트 검색 작업을 수행합니다."""
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

        :param start_date: 시작 날짜
        :param end_date: 종료 날짜
        """
        self._retry_action(self._perform_rma_return, start_date, end_date)

    def _perform_rma_return(self, start_date: str, end_date: str) -> None:
        """실제 RMA 반환 프로세스를 수행합니다."""
        # 기존의 process_rma_return 함수 내용을 여기에 구현
        # 각 단계에서 WebDriverWait를 사용하여 요소가 나타날 때까지 기다림

    def _retry_action(self, action, *args):
        """
        지정된 횟수만큼 작업을 재시도합니다.

        :param action: 수행할 작업 (함수)
        :param args: 작업에 전달할 인자들
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
        """웹 드라이버를 종료합니다."""
        self.driver.quit()


def initialize_and_login(
    config: Dict[str, Any], username: str, password: str
) -> WebCrawler:
    """
    WebCrawler를 초기화하고 로그인합니다.

    :param config: 설정 정보
    :param username: 사용자 이름
    :param password: 비밀번호
    :return: 초기화된 WebCrawler 인스턴스
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
