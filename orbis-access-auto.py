# Description: This script applies batch search on the Orbis website
# Python Version: 3.9.5
# Python Libraries: selenium, pandas, yaml

import yaml
import time
import pandas as pd
import numpy as np
import multiprocessing
import multiprocessing.pool
from os import path
from os import environ
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from crawl_data import prepare_data
import hashlib
import logging
import pathlib

# ORBIS ELEMENT VARIABLES
LOGIN_BUTTON = "/html/body/div[2]/div/div[1]/div[1]/div[1]/form/fieldset/div[3]/input"
DRAG_DROP_BUTTON = "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a"
SELECT_FILE_BUTTON = "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[1]/div[1]/input[1]"
UPLOAD_BUTTON = "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[2]/p/a[2]"
FIELD_SEPERATOR = "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input"
APPLY_BUTTON = "/html/body/section[2]/div[3]/div/form/div[3]/div[2]/input"
SEARCH_PROGRESS_BAR = "/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[1]"
VIEW_RESULTS_BUTTON = "/html/body/section[2]/div[1]/div[2]/ul/li[1]/a"
ADD_REMOVE_COLUMNS_VIEW = '//*[@id="main-content"]/div/div[2]/div[1]/a'

FINANCIAL_DATA_BUTTON = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[7]/div'
KEY_FINANCIAL_DATA = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[7]/ul/li[2]'
OP_REVENUE_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.OPRE:UNIVERSAL"]/div[1]/span[2]'
PL_BEFORE_TAX_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.PLBT:UNIVERSAL"]/div[1]'
PL_FOR_PERIOD_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.PL:UNIVERSAL"]/div[1]'
CASH_FLOW_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.CF:UNIVERSAL"]/div[1]'
TOTAL_ASSETS_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.TOAS:UNIVERSAL"]/div[1]'
NUMBER_OF_EMPLOYEES_SETTINGS = '/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[3]/div/ul/li[2]/div[1]'
OPERATING_PL_SETTINS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.OPPL:IND"]/div[1]'
GROSS_PROFIT = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.GROS:IND"]/div[1]'
SALES_SETTINGS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.TURN:IND"]/div[1]'
ABSOLUTE_IN_COLUMN_OP = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[1]/div/table/tbody/tr/td[2]/a'
# all selections from the left panel
USER_SELECTIONS_PANEL = '//*[@id="main-content"]/div/div[2]/div[2]/div[1]'
SCROLLABLE_XPATH = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div'
SCROLLABLE_XPATH_IN_SECOND_OPTION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[3]/div/div'

ANNUAL_DATA_LIST = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div/ul'
MILLION_UNITS = '//*[@id="id-currency-menu-popup"]/ul[1]/li[4]'
OP_REVENUE_OK_BUTTON = '/html/body/section[2]/div[6]/div[3]/a[2]'
CURRENY_DROPDOWN = '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[4]/a'
DROPDOWN_APPLY = '//*[@id="id-currency-menu-popup"]/div/a[2]'

EXCEL_EXPORT_NAME_FIELD = '//*[@id="component_FileName"]'
MAIN_DIV = '//*[@id="main-content"]'

SEARCH_INPUT_ADD_RM_COLUMNS = '//*[@id="Search"]'
SEARCH_ICON_ADD_RM_COLUMNS = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[1]/div[2]/div/span'
# Other company ID number
CIK_NUMBER_VIEW = '//*[@id="IDENTIFIERS*IDENTIFIERS.COMPANY_ID_NUMBER:UNIVERSAL"]/div[2]/span'
# when adding Other company ID number column to data view
POPUP_SAVE_BUTTON = '/html/body/section[2]/div[6]/div[3]/a[2]'
CITY_COLUMN = '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.CITY:UNIVERSAL"]/div[2]/span'
COUNTRY_COLUMN = '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.COUNTRY:UNIVERSAL"]/div[2]/span'
CONTACT_INFORMATION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[3]/div'
BVD_SECTORS = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.BVD_SECTOR_CORE_LABEL:UNIVERSAL"]/div[2]/span'
US_SIC_PRIMARY_CODES = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_PRIMARY_CODE:UNIVERSAL"]/div[2]/span'
US_SIC_SECONDARY_CODES = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_SECONDARY_CODE:UNIVERSAL"]/div[2]/span'
DELISTING_NOTE = '//*[@id="STOCKDATA*STOCKDATA.SD_DELISTED_NOTE:UNIVERSAL"]/div[2]/span'
DESCRIPTION_HISTORY = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.DESCRIPTION_HISTORY:UNIVERSAL"]/div[2]/span'
HISTORY = '//*[@id="OVERVIEW*OVERVIEW.OVERVIEW_HISTORY:UNIVERSAL"]/div[2]/span'
IDENTIFICATION_NUMBER_VIEW = '/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]/div'

TRADE_DESC = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.TRADE_DESCRIPTION_EN:UNIVERSAL"]/div[1]/span'
BVD_ID_NUMBER_ADD = '//*[@id="IDENTIFIERS*IDENTIFIERS.BVD_ID_NUMBER:UNIVERSAL"]'
ORBIS_ID_NUMBER_ADD = '//*[@id="IDENTIFIERS*IDENTIFIERS.ORBISID:UNIVERSAL"]'
OWNERSHIP_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div'
SHAREHOLDERS_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div'
GUO_NAME_INFO = '//*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span'
GUO_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[4]/div'
IMMEDIATE_PARENT_COMPANY_NAME = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[3]/div'
ISH_NAME = '//*[@id="ISH*ISH.ISH_NAME:UNIVERSAL"]'
APPLY_CHANGES_BUTTON = '//*[@id="main-content"]/div/div[3]/form/div/input[2]'
EXCEL_BUTTON = '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a'
EXPORT_BUTTON = '/html/body/section[2]/div[5]/form/div[2]/a[2]'
POPUP_DOWNLOAD_BUTTON = '/html/body/section[2]/div[6]/div[3]/a'


# initialize logger

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# log_file = pathlib.Path(getenv("LOG_DIR")).joinpath(rf'{timestamp}_orbis.log')
# get current working directory
log_file = pathlib.Path.cwd().joinpath(rf'logs/{timestamp}_orbis.log')


logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger()


class Orbis:

    def __init__(self, offline=False):

        if environ.get("LOCAL_DEV") == 'True':
            config = self.read_config('./config/config.yaml')
            self.executable_path = config['selenium']['executable_path']
            self.orbis_access_url = config['orbis']['urls']['access']
            self.orbis_batch_search_url = config['orbis']['urls']['batch']
            self.orbis_logout_url = config['orbis']['urls']['logout']
            self.email_address = config['orbis']['email']
            self.data_dir = config['data']['path']
            self.password = config['orbis']['password']
            self.data_source = config['data']['source']
            self.license_data = path.join(self.data_dir, self.data_source)
        else:
            self.executable_path = environ.get("CHROMEDRIVER_PATH")
            self.orbis_access_url = environ.get("ORBIS_ACCESS_URL")
            self.orbis_batch_search_url = environ.get("ORBIS_BATCH_SEARCH_URL")
            self.orbis_logout_url = environ.get("ORBIS_LOGOUT_URL")
            self.email_address = environ.get("ORBIS_EMAIL_ADDRESS")
            self.password = environ.get("ORBIS_PASSWORD")
            self.data_dir = environ.get("DATA_DIR")
            self.license_data = environ.get(
                "DATA_DIR") + environ.get("DATA_SOURCE")

        self.driver = None
        self.offline = offline
        self.variables = {
            "Operating revenue (Turnover)": OP_REVENUE_SETTINGS,
            "Sales": SALES_SETTINGS,
            "Gross profit": GROSS_PROFIT,
            "Operating P/L [=EBIT]": OPERATING_PL_SETTINS,
            "P/L before tax": PL_BEFORE_TAX_SETTINGS,
            "P/L for period [=Net income]": PL_FOR_PERIOD_SETTINGS,
            "Cash flow": CASH_FLOW_SETTINGS,
            "Total assets": TOTAL_ASSETS_SETTINGS,
            "Number of employees": NUMBER_OF_EMPLOYEES_SETTINGS,
            "Trade description (English)": TRADE_DESC,
            "BvD sectors": BVD_SECTORS,

        }

    def __exit__(self, exc_type, exc_value, traceback):

        if not self.offline:
            self.logout()
            if self.driver is not None:
                return self.driver.close()

        return None

    def __enter__(self):
        if not self.offline:
            logger.debug("Starting chrome driver...")
            service = ChromeService(executable_path=self.executable_path)
            self.chrome_options = webdriver.ChromeOptions()
            prefs = {'download.default_directory': self.data_dir}
            # add user agent to avoid bot detection
            self.chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36")
            if environ.get("LOCAL_DEV") != "True":
                self.chrome_options.add_argument("--headless")
                self.chrome_options.add_argument("--window-size=1920,1080")
                self.chrome_options.add_experimental_option("detach", True)

            self.chrome_options.add_experimental_option('prefs', prefs)
            self.driver = webdriver.Chrome(
                service=service, options=self.chrome_options)
            logger.debug("Chrome driver started")
            self.login()
        return self

    def get_financial_columns(self):
        financial_columns = list(self.variables.keys())
        financial_columns = financial_columns[:len(
            financial_columns) - 2]  # remove the last two columns
        return financial_columns

    def drop_columns(self, df, regex):
        columns_to_drop = df.filter(regex=regex).columns
        logger.debug("Dropping columns: ", columns_to_drop)
        return df.drop(columns=columns_to_drop)

    def read_config(self, path):
        # read from yaml config file
        with open(path, "r") as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                raise exc
        return config

    def login(self):
        # login to the web site
        self.driver.get(self.orbis_access_url)
        # find with XPath the username and password fields
        username = self.driver.find_element(By.ID, "user")
        password = self.driver.find_element(By.ID, "pass")
        # fill the fields with given values
        username.send_keys(self.email_address)
        password.send_keys(self.password)
        # find with XPath the login button and click it
        login_button = self.driver.find_element(By.XPATH, LOGIN_BUTTON)
        # click the login button
        login_button.click()
        logger.debug("Logging in Orbis ...")

    def logout(self):
        # logout from the web site
        time.sleep(3)
        logger.debug("Logging out Orbis ...")
        self.driver.get(self.orbis_logout_url)

    def scroll_to_bottom(self):
        # scroll to the bottom of the page
        # only used after add/remove column step
        self.driver.execute_script(
            "document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")

    def wait_until_clickable(self, xpath):
        # wait until the element is clickable
        WebDriverWait(
            self.driver,
            30 *
            60).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 xpath)))

    def read_xlxs_file(self, file_path, sheet_name=''):
        if sheet_name == '':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df

    def search_and_add(self, item):

        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.clear()
        search_input.send_keys(item)
        search_input.send_keys(Keys.RETURN)
        time.sleep(5)
        logger.debug(f"Searching for {item} ...")

    def check_checkboxes(self, field=''):
        try:

            scrollable = self.driver.find_element(By.XPATH, SCROLLABLE_XPATH)
            scroll_position = self.driver.execute_script(
                "return arguments[0].scrollTop", scrollable)
            # print(f"scroll position is {scroll_position}")
            height_of_scrollable = self.driver.execute_script(
                "return arguments[0].scrollHeight", scrollable)
            # print(f"height of scrollable is {height_of_scrollable}")
            scroll_amount = 100
            while scroll_position < height_of_scrollable:
                scrollable = self.driver.find_element(
                    By.XPATH, SCROLLABLE_XPATH)
                checkboxes = scrollable.find_elements(
                    By.CSS_SELECTOR, "input[type='checkbox']")
                for checkbox in checkboxes:
                    try:
                        if checkbox.get_attribute("checked") == "true":
                            continue
                        checkbox.click()
                    except Exception:
                        # print(f"exception on checkbox {e}")
                        continue
                self.driver.execute_script(
                    f"arguments[0].scrollTo(0,{scroll_position})", scrollable)
                scroll_position += scroll_amount
                logger.debug(f"{field} Scrolling to {scroll_position} ...")
                time.sleep(0.5)
        except Exception as e:
            logger.debug(f"Exception on check_checkboxes {e}")

    # select_all_years function is used to get the operating revenue data in
    # millions for all available years
    def select_all_years(self, field='', is_checked=False):
        # click to finanacial data column
        logger.debug(f"Selecting all years for {field} ...")
        if not is_checked:

            retry = 0
            max_retry = 5

            while retry < max_retry:
                try:
                    self.wait_until_clickable(ABSOLUTE_IN_COLUMN_OP)
                    self.driver.find_element(
                        By.XPATH, ABSOLUTE_IN_COLUMN_OP).click()
                    self.wait_until_clickable(ANNUAL_DATA_LIST)
                    self.check_checkboxes(field)
                    self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
                    self.driver.find_element(
                        By.XPATH, OP_REVENUE_OK_BUTTON).click()
                    break
                except Exception as e:
                    logger.debug(f"exception on click {e} retrying... {retry}")
                    retry += 1
                    time.sleep(5)

    def batch_search(self, input_file, process_name=''):

        logger.debug(f"Starting batch search for {process_name} ...")

        if not path.exists(input_file):
            logger.debug(
                f"{process_name}: input file {input_file} does not exist")
            return

        excel_output_file_name = path.basename(input_file).split(".")[0]

        self.driver.get(self.orbis_batch_search_url)

        self.wait_until_clickable(DRAG_DROP_BUTTON)
        self.driver.find_element(By.XPATH, DRAG_DROP_BUTTON).click()

        file_input = self.driver.find_element(By.XPATH, SELECT_FILE_BUTTON)
        file_input.send_keys(input_file)

        self.wait_until_clickable(UPLOAD_BUTTON)

        self.driver.find_element(By.XPATH, UPLOAD_BUTTON).click()

        self.wait_until_clickable(FIELD_SEPERATOR)

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).clear()
        time.sleep(2)

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).send_keys(";")

        self.driver.find_element(
            By.XPATH, FIELD_SEPERATOR).send_keys(
            Keys.RETURN)

        logger.debug(f"{process_name} field seperator is cleared")
        time.sleep(2)
        self.wait_until_clickable(APPLY_BUTTON)

        self.driver.find_element(By.XPATH, APPLY_BUTTON).click()

        time.sleep(5)
        WebDriverWait(
            self.driver,
            30 *
            60).until(
            EC.invisibility_of_element_located(
                (By.XPATH,
                 SEARCH_PROGRESS_BAR)))

        try:
            warning_message = self.driver.find_element(
                By.XPATH, SEARCH_PROGRESS_BAR)
            while warning_message.text == "Search is not finished":
                time.sleep(5)
                warning_message = self.driver.find_element(
                    By.XPATH, SEARCH_PROGRESS_BAR)
                logger.debug(f"{process_name}: {warning_message.text}")
            time.sleep(5)
        except Exception as e:
            logger.debug(
                f"{process_name}: search is not finished: stale element exception {e}")

        self.wait_until_clickable(VIEW_RESULTS_BUTTON)
        view_result_sub_url = self.driver.find_element(
            By.XPATH, VIEW_RESULTS_BUTTON)
        view_result_sub_url.send_keys(Keys.RETURN)

        # this is used to wait until processing overlay is gone
        try:
            main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
            main_content_style = main_content_div.value_of_css_property(
                'max-width')

            while main_content_style != "none":
                main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
                main_content_style = main_content_div.value_of_css_property(
                    'max-width')
                logger.debug(
                    f"{process_name}main content style is {main_content_style}")
                time.sleep(0.5)
        except Exception as e:
            logger.debug(e)

        time.sleep(5)
        self.wait_until_clickable(ADD_REMOVE_COLUMNS_VIEW)
        self.driver.find_element(By.XPATH, ADD_REMOVE_COLUMNS_VIEW).click()

        self.wait_until_clickable(CONTACT_INFORMATION)
        self.driver.find_element(By.XPATH, CONTACT_INFORMATION).click()

        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.send_keys("City")
        search_input.send_keys(Keys.RETURN)
        logger.debug(f"{process_name} city is searched")

        # add city
        self.wait_until_clickable(CITY_COLUMN)
        self.driver.find_element(By.XPATH, CITY_COLUMN).click()
        logger.debug(f"{process_name} city is added")

        self.wait_until_clickable(POPUP_SAVE_BUTTON)
        self.driver.find_element(By.XPATH, POPUP_SAVE_BUTTON).click()
        logger.debug(f"{process_name} popup save button is clicked")

        # self.driver.refresh()
        search_input.clear()
        time.sleep(1)

        search_input.send_keys("Country")
        search_input.send_keys(Keys.RETURN)
        logger.debug(f"{process_name} country is searched")

        self.wait_until_clickable(COUNTRY_COLUMN)
        self.driver.find_element(By.XPATH, COUNTRY_COLUMN).click()
        logger.debug(f"{process_name} country is added")

        search_input.clear()
        time.sleep(1)

        # delisting note  : required for the M&A activities note

        search_input.send_keys("Delisting note")
        search_input.send_keys(Keys.RETURN)
        logger.debug(f"{process_name} delisting note is searched")

        self.wait_until_clickable(DELISTING_NOTE)
        self.driver.find_element(By.XPATH, DELISTING_NOTE).click()

        search_input.clear()
        time.sleep(1)
        # identification number column

        self.wait_until_clickable(IDENTIFICATION_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, IDENTIFICATION_NUMBER_VIEW).click()
        logger.debug(f"{process_name} identification number is clicked")
        # search input for Other company ID number (CIK number)
        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        # try:
        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.send_keys("Other company ID number")
        search_input.send_keys(Keys.RETURN)
        logger.debug(f"{process_name} other company id number is searched")

        self.wait_until_clickable(CIK_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, CIK_NUMBER_VIEW).click()
        logger.debug(f"{process_name} cik number is added")

        self.wait_until_clickable(POPUP_SAVE_BUTTON)
        self.driver.find_element(By.XPATH, POPUP_SAVE_BUTTON).click()
        logger.debug(f"{process_name} popup save button is clicked")

        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.clear()
        search_input.send_keys("US SIC, primary code(s)")
        search_input.send_keys(Keys.RETURN)
        self.wait_until_clickable(US_SIC_PRIMARY_CODES)
        self.driver.find_element(By.XPATH, US_SIC_PRIMARY_CODES).click()
        self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
        self.driver.find_element(By.XPATH, OP_REVENUE_OK_BUTTON).click()
        logger.debug(f"{process_name} us sic primary codes is added")

        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.clear()
        search_input.send_keys("US SIC, secondary code(s)")
        search_input.send_keys(Keys.RETURN)
        self.wait_until_clickable(US_SIC_SECONDARY_CODES)
        self.driver.find_element(By.XPATH, US_SIC_SECONDARY_CODES).click()
        self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
        self.driver.find_element(By.XPATH, OP_REVENUE_OK_BUTTON).click()
        logger.debug(f"{process_name} us sic secondary codes is added")

        search_input.clear()
        search_input.send_keys("Description and history")
        search_input.send_keys(Keys.RETURN)
        self.wait_until_clickable(DESCRIPTION_HISTORY)
        self.driver.find_element(By.XPATH, DESCRIPTION_HISTORY).click()

        search_input.clear()
        search_input.send_keys("History")
        search_input.send_keys(Keys.RETURN)
        self.wait_until_clickable(HISTORY)
        self.driver.find_element(By.XPATH, HISTORY).click()

        # self.driver.refresh()
        search_input.clear()
        time.sleep(1)
        self.wait_until_clickable(IDENTIFICATION_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, IDENTIFICATION_NUMBER_VIEW).click()
        logger.debug(f"{process_name} identification number is clicked")
        # add BVD ID number column

        self.wait_until_clickable(BVD_ID_NUMBER_ADD)
        self.driver.find_element(By.XPATH, BVD_ID_NUMBER_ADD).click()
        logger.debug(f"{process_name} bvd id number is added")
        # add ORBIS ID number column

        self.wait_until_clickable(ORBIS_ID_NUMBER_ADD)
        self.driver.find_element(By.XPATH, ORBIS_ID_NUMBER_ADD).click()
        logger.debug(f"{process_name} orbis id number is added")

        time.sleep(2)
        # scroll down within in panel
        self.scroll_to_bottom()
        time.sleep(1)

        # Ownership Data >
        # //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div

        self.wait_until_clickable(OWNERSHIP_COLUMN)
        self.driver.find_element(By.XPATH, OWNERSHIP_COLUMN).click()
        logger.debug(f"{process_name} ownership data is added")

        # scroll down within in panel
        self.scroll_to_bottom()
        time.sleep(1)

        # Shareholders
        # //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div

        self.wait_until_clickable(SHAREHOLDERS_COLUMN)
        self.driver.find_element(By.XPATH, SHAREHOLDERS_COLUMN).click()
        logger.debug(f"{process_name} shareholders is added")

        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Information

        self.wait_until_clickable(GUO_COLUMN)
        self.driver.find_element(By.XPATH, GUO_COLUMN).click()
        logger.debug(f"{process_name} global ultimate owner is added")

        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Name
        # //*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span

        self.wait_until_clickable(GUO_NAME_INFO)
        self.driver.find_element(By.XPATH, GUO_NAME_INFO).click()
        logger.debug(f"{process_name} global ultimate owner name is added")

        self.scroll_to_bottom()
        time.sleep(1)

        self.wait_until_clickable(IMMEDIATE_PARENT_COMPANY_NAME)
        self.driver.find_element(
            By.XPATH, IMMEDIATE_PARENT_COMPANY_NAME).click()
        logger.debug(f"{process_name} immediate parent company name is added")

        self.wait_until_clickable(ISH_NAME)
        self.driver.find_element(By.XPATH, ISH_NAME).click()
        logger.debug(f"{process_name} ish name is added")

        attempts = 0
        max_attempts = 10
        while True:
            if not self.driver.title:
                logger.debug(
                    f"{process_name} page title is None retrying... {attempts}")
                self.driver.refresh()
                attempts += 1
                if attempts > max_attempts:
                    break
                time.sleep(1)
            else:
                logger.debug(
                    f"{process_name} page title is {self.driver.title}")
                logger.debug(
                    f"{process_name} page url is {self.driver.current_url}")
                logger.debug(
                    f"{process_name} exit from while loop self.driver.title not None")
                break

        # logs = self.driver.get_log('browser')
        # for log in logs:
        #     if log['level'] == 'SEVERE':
        #         print(log)
        non_financial_items = list(
            set(self.variables.keys()) - set(self.get_financial_columns()))
        is_checked = False
        for item, xpath in self.variables.items():
            retries = 0
            max_retries = 4
            item_already_selected = False
            while retries < max_retries:
                try:
                    # check stale element reference exception
                    self.search_and_add(item)
                    user_selections_panel = self.driver.find_element(
                        By.XPATH, USER_SELECTIONS_PANEL)
                    all_selected_fields = user_selections_panel.find_elements(
                        By.CLASS_NAME, 'label')
                    if item in [field.text for field in all_selected_fields]:
                        logger.debug(
                            f"{process_name} item already selected : {item}")
                        item_already_selected = True
                        break
                    self.wait_until_clickable(xpath)
                    self.driver.find_element(By.XPATH, xpath).click()
                    time.sleep(3)
                    if item in non_financial_items:
                        self.select_all_years(
                            field=item, is_financial_data=False, is_checked=is_checked)
                    else:
                        self.select_all_years(
                            field=item, is_checked=is_checked)
                    logger.debug(
                        f"{process_name} item : {item} xpath : {xpath} ")
                    break
                except Exception:
                    if item_already_selected:
                        logger.debug(
                            f"{process_name} exception happened but item already selected ; skipping  {item} ")
                        break
                    retries += 1
                    logger.debug(
                        f"{process_name} stale element reference exception, trying again {retries} : {item}")
            is_checked = True
        logger.debug(
            f"{process_name} all items are added: {self.variables.keys()}")

        # apply changes button

        self.wait_until_clickable(APPLY_CHANGES_BUTTON)
        self.driver.find_element(By.XPATH, APPLY_CHANGES_BUTTON).click()

        time.sleep(3)

        # currency select unit
        # self.wait_until_clickable(CURRENY_DROPDOWN)
        self.driver.find_element(By.XPATH, CURRENY_DROPDOWN).click()

        time.sleep(7)
        self.wait_until_clickable(MILLION_UNITS)
        self.driver.find_element(By.XPATH, MILLION_UNITS).click()
        # select million

        time.sleep(3)
        # click apply from dropdown

        self.driver.find_element(By.XPATH, DROPDOWN_APPLY).click()
        time.sleep(3)

        WebDriverWait(
            self.driver,
            30 *
            60).until(
            EC.text_to_be_present_in_element(
                (By.XPATH,
                 EXCEL_BUTTON),
                'Excel'))
        self.driver.find_element(By.XPATH, EXCEL_BUTTON).click()
        logger.debug(f"{process_name} excel button is clicked")

        self.wait_until_clickable(EXCEL_EXPORT_NAME_FIELD)
        excel_filename_input = self.driver.find_element(
            By.XPATH, EXCEL_EXPORT_NAME_FIELD)
        excel_filename_input.clear()
        excel_filename_input.send_keys(excel_output_file_name)
        logger.debug(f"{process_name} excel file name is entered")

        # excel_output_file_name.send_keys(Keys.RETURN)
        time.sleep(2)
        WebDriverWait(
            self.driver,
            30 *
            60).until(
            EC.text_to_be_present_in_element(
                (By.XPATH,
                 EXPORT_BUTTON),
                'Export'))
        self.driver.find_element(By.XPATH, EXPORT_BUTTON).click()
        logger.debug(f"{process_name} export button is clicked")

        self.wait_until_clickable(POPUP_DOWNLOAD_BUTTON)
        while True:
            download_button = self.driver.find_element(
                By.XPATH, POPUP_DOWNLOAD_BUTTON)
            if download_button.value_of_css_property(
                    "background-color") == "rgba(0, 20, 137, 1)":
                # download_button.send_keys(Keys.RETURN)
                logger.debug(
                    f"{process_name} waiting for data generation to download ")
                time.sleep(0.5)
                break
            else:
                time.sleep(2)

    # create a file likewe have data.csv file
    # company name;city;country;identifier
    def generate_data_for_guo(self, orig_orbis_data, file):
        logger.debug(f"Generating data for guo... ")
        df = self.read_xlxs_file(orig_orbis_data, sheet_name='Results')
        df = df[['GUO - Name', 'City\nLatin Alphabet',
                 'Country', 'Other company ID number']]
        # drop rows where company name is null
        df = df[df['GUO - Name'].notna()]
        df.to_csv(
            file,
            index=False,
            header=[
                'company name',
                'city',
                'country',
                'identifier'],
            sep=';')
        logger.debug(f"Data for guo is generated... ")

    def generate_data_for_ish(self, orig_orbis_data, file):
        logger.debug(f"Generating data for ish... ")
        df = self.read_xlxs_file(orig_orbis_data, sheet_name='Results')
        df = df[['ISH - Name', 'City\nLatin Alphabet',
                 'Country', 'Other company ID number']]
        df = df[df['ISH - Name'].notna()]
        df.to_csv(
            file,
            index=False,
            header=[
                'company name',
                'city',
                'country',
                'identifier'],
            sep=';')
        logger.debug(f"Data for ish is generated... ")

    def strip_new_lines(self, df, colunm_name='Licensee'):
        df[colunm_name] = df[colunm_name].apply(lambda x: x.strip('\n'))
        return df

    def prepare_data(self, df, df_orbis):
        logger.debug(f"Preparing data... ")
        related_data = df[['Licensee',
                           'Licensee CIK 1_cleaned',
                           'Licensor',
                           'Agreement Date']]
        # drop rows where Licensee CIK 1_cleaned is null
        related_data = related_data[related_data['Licensee CIK 1_cleaned'].notna(
        )]

        related_data['Agreement Date'] = pd.to_datetime(
            related_data['Agreement Date'])
        related_data['Financial Period'] = related_data['Agreement Date'].dt.year - 1
        related_data = related_data.rename(
            columns={'Licensee CIK 1_cleaned': 'CIK'})
        df_orbis = df_orbis.rename(columns={'Other company ID number': 'CIK'})
        df_merged = df_orbis.merge(related_data, on='CIK')

        logger.debug(f"Data is merged on CIK numbers ... ")

        # drop columns which ends with Last avail. yr
        df_merged = self.drop_columns(df_merged, 'Last avail. yr$')
        for index, r in df_merged.iterrows():
            year = r['Financial Period']
            try:
                number_of_employees = r[f'Number of employees\n{year}']
                df_merged.at[index,
                             'Number of employees (in Financial Period)'] = number_of_employees
                for f_colunm in self.get_financial_columns():
                    revenue_info = r[f'{f_colunm}\nm USD {year}']
                    df_merged.at[index, f'{f_colunm}\nm USD'] = revenue_info

            except KeyError as ke:
                logger.debug(
                    f"KeyError: {ke} for index {index} and year {year}")

        # drop columns which ends with a year
        df_merged = self.drop_columns(df_merged, '[0-9]$')
        logger.debug(f"Data is prepared... ")
        return df_merged

    def to_xlsx(self, df, file_name):
        df.to_excel(file_name)


def run_batch_search(input_file, process_name):
    # join path to input file
    logger.debug(
        f"Running batch search for file {input_file} and process name {process_name}")
    with Orbis() as orbis:
        logger.debug(f"Data directory is {orbis.data_dir}")
        logger.debug(f"Input file is {path.join(orbis.data_dir, input_file)}")
        orbis.batch_search(path.join(orbis.data_dir, input_file), process_name)

# offline_data_aggregation is used to generate data by considering GUO of companies
# this data needs to be generated when we have new data from Orbis (refer
# workflow in README.md)


def generate_data_for_guo(orbis_data_file, output_file):
    logger.debug(
        f"Generating data for GUO for file {orbis_data_file} and saving to {output_file}")
    with Orbis(offline=True) as orbis:
        output_file = path.join(orbis.data_dir, output_file)
        orbis_data_file = path.join(orbis.data_dir, orbis_data_file)
        orbis.generate_data_for_guo(
            orig_orbis_data=orbis_data_file,
            file=output_file)


def generate_data_for_ish(orbis_data_file, output_file):
    logger.debug(
        f"Generating data for ISH for file {orbis_data_file} and saving to {output_file}")
    with Orbis(offline=True) as orbis:
        output_file = path.join(orbis.data_dir, output_file)
        orbis_data_file = path.join(orbis.data_dir, orbis_data_file)
        orbis.generate_data_for_ish(
            orig_orbis_data=orbis_data_file,
            file=output_file)


# aggregate_data is used to aggregate data by considering Licensee of companies
# (refer workflow in README.md)
def aggregate_data(orbis_file, aggregated_output_file):
    logger.debug(
        f"Aggregating data for file {orbis_file} and saving to {aggregated_output_file}")
    with Orbis(offline=True) as orbis:
        # data processing and augmentation
        # after orbis search step following needs to run
        orbis_file = path.join(orbis.data_dir, orbis_file)
        aggregate_output_file = path.join(
            orbis.data_dir, aggregated_output_file)
        df_licensee_data = orbis.read_xlxs_file(orbis.license_data)
        df_orbis_data = orbis.read_xlxs_file(orbis_file, sheet_name='Results')
        df_licensee = orbis.strip_new_lines(df_licensee_data)
        df_result = orbis.prepare_data(df_licensee, df_orbis_data)
        orbis.to_xlsx(df_result, aggregate_output_file)
        logger.debug(
            f"Data is aggregated and saved to {aggregate_output_file}")


def generate_unique_id(company_name, n):
    logger.debug(f"Generating unique id for {company_name}")
    sha256 = hashlib.sha256()
    sha256.update(company_name.encode())
    return sha256.hexdigest()[:n]


def post_process_data(excel_file):
    # create another column for unique identifier with hash for column 1 however no negative values
    # drop all rows where Orbis ID number is null
    # get data/ folder path and append file name to it
    get_dir_path = ''
    logger.debug(f"Post processing data for {excel_file}")
    with Orbis(offline=True) as orbis:
        get_dir_path = orbis.data_dir

    # append data and file name to it
    excel_file = path.join(get_dir_path, excel_file)
    if not path.exists(excel_file):
        logger.debug(f"{excel_file} does not exist")
        return
    logger.debug(f"post process data for {excel_file}")
    df = pd.read_excel(excel_file, sheet_name='Results')
    df = df[df['Orbis ID number'].notna()]
    # drop Unnamed: 0 column and duplicate columns
    df = df.drop(columns=['Unnamed: 0'])

    # remove duplicate columns however keep first occurance
    df = df.loc[:, ~df.columns.duplicated()]

    # fix lentgth uniquie identifier
    df['Unique identifier'] = df['Orbis ID number'].apply(
        lambda x: generate_unique_id(str(x), 10))
    df.to_excel(excel_file)
    logger.debug(f"post process data for {excel_file} completed")


def run_in_parallel_generic(function, starmap, args):
    number_of_processes = multiprocessing.cpu_count()
    if starmap:
        with multiprocessing.pool.ThreadPool(number_of_processes) as pool:
            pool.starmap(function, args)
    else:
        with multiprocessing.pool.ThreadPool(number_of_processes) as pool:
            pool.imap(function, args)
            pool.close()
            pool.join()


def save_screenshot(driver, file_name):
    driver.save_screenshot(f'{file_name}.png')
    logger.debug(f"Screenshot saved to {file_name}.png")


if __name__ == "__main__":
    timestamp = datetime.now().strftime("%d_%m_%Y")

    # Step 1
    # --> crawl_data.py should generate data in data/data.csv

    # crawl_data: prepare_data
    # generates csv file for licensee
    prepare_data(
        "sample_data.xlsx",
        f"orbis_data_licensee_{timestamp}.csv",
        is_licensee=True)

    # # generates csv file for licensor
    prepare_data(
        "sample_data.xlsx",
        f"orbis_data_licensor_{timestamp}.csv",
        is_licensee=False)

    # time.sleep(4) # wait for 4 seconds for data to be saved in data folder

    # # Step 2
    # # --> data/data.csv needs to be uploaded to Orbis to start batch search
    # run_batch_search(config_path, f"orbis_d.csv") # Todo: this csv file
    # needs to come from crawl_data.py
    run_in_parallel_generic(run_batch_search,
                            True,
                            [(f"orbis_data_licensee_{timestamp}.csv",
                              "turtle"),
                             (f"orbis_data_licensor_{timestamp}.csv",
                              "viper")])

    # run_batch_search(config_path, f"orbis_data_{timestamp}.csv") # Todo: this csv file needs to come from crawl_data.py
    # # # # --> after batch search is completed, data downloaded from Orbis

    # time.sleep(2) # wait for 2 seconds for data to be saved in data folder

    # # # # Step 3
    # # # # --> generate_data_for_guo to generate data by considering GUO of companies

    run_in_parallel_generic(generate_data_for_guo,
                            True,
                            [(f"orbis_data_licensee_{timestamp}.xlsx",
                              f"orbis_data_licensee_guo_{timestamp}.csv"),
                             (f"orbis_data_licensor_{timestamp}.xlsx",
                              f"orbis_data_licensor_guo_{timestamp}.csv")])

    run_in_parallel_generic(run_batch_search,
                            True,
                            [(f"orbis_data_licensee_guo_{timestamp}.csv",
                              "turtle_guo"),
                             (f"orbis_data_licensor_guo_{timestamp}.csv",
                              "viper_guo")])

    time.sleep(2)  # wait for 2 seconds for data to be saved in data folder

    run_in_parallel_generic(generate_data_for_ish,
                            True,
                            [(f"orbis_data_licensee_{timestamp}.xlsx",
                              f"orbis_data_licensee_ish_{timestamp}.csv"),
                             (f"orbis_data_licensor_{timestamp}.xlsx",
                              f"orbis_data_licensor_ish_{timestamp}.csv")])

    run_in_parallel_generic(run_batch_search,
                            True,
                            [(f"orbis_data_licensee_ish_{timestamp}.csv",
                              "turtle_ish"),
                             (f"orbis_data_licensor_ish_{timestamp}.csv",
                              "viper_ish")])
    # # # # # Step 5
    time.sleep(2)  # wait for 2 seconds for data to be saved in data folder

    run_in_parallel_generic(aggregate_data,
                            True,
                            [(f"orbis_data_licensee_{timestamp}.xlsx",
                              f"orbis_aggregated_data_licensee_{timestamp}.xlsx"),
                             (f"orbis_data_licensor_{timestamp}.xlsx",
                              f"orbis_aggregated_data_licensor_{timestamp}.xlsx")])

    run_in_parallel_generic(post_process_data,
                            False,
                            [(f"orbis_aggregated_data_{timestamp}.xlsx"),
                             (f"orbis_aggregated_data_licensee_{timestamp}.xlsx"),
                                (f"orbis_aggregated_data_licensor_{timestamp}.xlsx"),
                                (f"orbis_aggregated_data_guo_{timestamp}.xlsx"),
                                (f"orbis_data_guo_{timestamp}.xlsx"),
                                (f"orbis_data_licensee_ish_{timestamp}.xlsx"),
                                (f"orbis_data_licensor_ish_{timestamp}.xlsx"),
                                (f"orbis_data_licensee_{timestamp}.xlsx"),
                                (f"orbis_data_licensee_guo_{timestamp}.xlsx"),
                                (f"orbis_data_licensor_{timestamp}.xlsx"),
                                (f"orbis_data_licensor_guo_{timestamp}.xlsx"),
                                (f"orbis_data_{timestamp}.xlsx")])
