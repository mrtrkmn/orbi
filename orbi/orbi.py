import concurrent.futures
import csv
import hashlib
import logging
import multiprocessing
import multiprocessing.pool
import os
import pathlib
import sys
import time
from datetime import datetime
from os import environ, path
from threading import Thread
import requests 

import pandas as pd
import yaml
from bs4 import BeautifulSoup
from crawl import create_input_file_for_orbis_batch_search
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# from slack_sdk import WebClient
# from slack_sdk.errors import SlackApiError
from variables import *

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)


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



##### Orbis #####

class Orbis:
    """
    Orbis class is used to handle connections to Orbis database
    :param offline: if True, the class will not attempt to connect to Orbis

    To login manually follow this link: https://www.ub.tum.de/en/datenbanken/details/12630

    Batch search will be performed with provided data file in the config file.
    """

    def __init__(self, offline=False):
        """
        Initialize an instance of the Orbis class.

        :param offline (bool): If True, the class will not attempt to connect to the Orbis database.

        :return:
        None
        """

        if environ.get("LOCAL_DEV") == 'True':
            config = self.read_config(environ.get("CONFIG_PATH"))
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
        self.headers = "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
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
        """
        The `__exit__` method is used to exit a runtime context related to the class.


        :param exc_type: The type of exception raised.
        :param exc_value: The exception raised.
        :param traceback: The traceback associated with the exception.

        :return:
        None
        """
        if not self.offline:
            self.logout()
            if self.driver is not None:
                return self.driver.close()

        return None

    def __enter__(self):
        if not self.offline:
            logger.debug("Starting chrome driver...")
            self.chrome_options = webdriver.ChromeOptions()
            prefs = {'download.default_directory': self.data_dir}
            # add user agent to avoid bot detection
            self.chrome_options.add_argument(
                self.headers)
            if environ.get("LOCAL_DEV") != "True":
                self.chrome_options.add_argument("--headless")
            
            self.chrome_options.add_argument("--start-maximized")
            self.chrome_options.add_argument("--window-size=1920,1080")    
            self.chrome_options.add_experimental_option('prefs', prefs)
            self.driver = webdriver.Chrome(
                executable_path=self.executable_path, options=self.chrome_options)
            logger.debug("Chrome driver started")
            # self.slack_client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
            time.sleep(1)
            self.login()
        return self

    def get_financial_columns(self):
        """
        Get a list of financial columns.

        :return:
        financial_columns (list): A list of financial columns.
        """
        financial_columns = list(self.variables.keys())
        financial_columns = financial_columns[:len(
            financial_columns) - 2]  # remove the last two columns
        return financial_columns

    def drop_columns(self, df, regex):
        """
        Drop columns from a pandas dataframe based on a regular expression.

        :param df (pandas.DataFrame): The pandas dataframe to drop columns from.
        :param regex (str): The regular expression to match column names.

        :return pandas.DataFrame: The pandas dataframe with columns dropped.
        """
        columns_to_drop = df.filter(regex=regex).columns
        logger.debug("Dropping columns: ", columns_to_drop)
        return df.drop(columns=columns_to_drop)

    def read_config(self, config_path):
        """
        Read a YAML configuration file.

        :param config_path (str): The config_path to the YAML configuration file.

        :return:
        dict: A dictionary with the configuration settings.
        """
        with open(config_path, "r", encoding="UTF-8") as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                raise exc
        return config

    def login(self):
        """
        Logs in to the Orbis website using Selenium.
        """
        # Open the Orbis login page
        self.driver.get(self.orbis_access_url)

        # Find the username and password fields using their IDs
        username = self.driver.find_element(By.ID, "user")
        password = self.driver.find_element(By.ID, "pass")

        # Fill in the fields with the email and password
        username.send_keys(self.email_address)
        password.send_keys(self.password)

        # Find the login button using an XPath expression and click it
        login_button = self.driver.find_element(By.XPATH, LOGIN_BUTTON)
        login_button.click()

        # Log a message to indicate that the login is in progress
        logger.debug("Logging in to Orbis...")
        time.sleep(4)
       
        ERROR_MESSAGE = "/html/body/section[2]/div[2]/form/div[1]"
        try: 
            response = self.driver.find_element(By.XPATH, ERROR_MESSAGE)
            # notify slack that logging is not successful to Orbis  database
            print(response.text)
            # self.slack_client.chat_postMessage(channel="#idp-data-c", text=f"Error on logging into Orbis ... ERR_MSG: {response.text}")
        except NoSuchElementException:
            print("No error message found")


    def logout(self):
        # logout from the web site
        """
        Logs out of the Orbis website.

        Waits for 3 seconds before logging out to ensure that all actions have been completed.

        :return:
        None
        """

        time.sleep(5)

        logger.debug("Logging out Orbis ...")
        response = requests.get(self.orbis_logout_url)
        # self.driver.get(self.orbis_logout_url)
        print(f"Response status code from logging out is: {response.status_code}")

        time.sleep(5)


    def scroll_to_bottom(self):
        # scroll to the bottom of the page
        # only used after add/remove column step
        """
        Scrolls to the bottom of the hierarchy container on the current Orbis page.

        This method is typically called after adding or removing columns to ensure that all changes are reflected in the container.

        :return:
        None
        """
        self.driver.execute_script(
            "document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")


    def wait_until_clickable(self, xpath):
        # wait until the element is clickable
        """
        Waits until an element located by the given XPath is clickable.

        Raises a TimeoutException if the element is not clickable after 30 minutes.

        :param xpath (str): The XPath of the element to wait for.

        :raises TimeoutException: If the element is not clickable after 30 minutes.
        """
        try: 
            WebDriverWait(
                self.driver,
                30 *
                60).until(
                EC.element_to_be_clickable(
                (By.XPATH,
                    xpath)))
        except Exception as e: 
            print("Exception occurred in wait_until_clickable, logging out from sessions")
            self.logout()


    def read_xlxs_file(self, file_path, sheet_name=''):
        """
        Reads an xlsx file and returns a pandas DataFrame.

        :param file_path (str): The path to the xlsx file.
        :param sheet_name (str): The name of the sheet to read. If not specified, the first sheet is read.

        :return: A pandas DataFrame

        """
        if sheet_name == '':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df


    def search_and_add(self, item):
        """
        Searches for the given `item` and adds it to the list of columns.

        :param item: A string representing the name of the item to be searched for and added.

        :return: None
        """
        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input = self.driver.find_element(
            By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.clear()
        search_input.send_keys(item)
        search_input.send_keys(Keys.RETURN)
        time.sleep(5)
        logger.debug(f"Searching for {item} ...")


    def check_checkboxes(self, field=''):
        """
        Checks all checkboxes for a specific field in the Orbis webpage.

        :param field (str): The name of the field to check the checkboxes for.

        :return: None
        """
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
        """Selects the operating revenue data in millions for all available years.

        :param field (str): The name of the financial field to select. Defaults to an empty string.
        :param is_checked (bool): A flag indicating whether the checkbox for the given field is already checked. Defaults to False.

        :return:
        None.
        """
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


    def check_processing_overlay(self, process_name):
        # this is used to wait until processing overlay is gone
        """Waits for the processing overlay to disappear.

        :param process_name (str): A string identifying the process being waited for.

        :return:None
        """
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
                print(
                    f"{process_name} main content style is still NOT NONE {main_content_style}")
                time.sleep(0.5)
        except Exception as e:
            logger.debug(e)


    def batch_search(self, input_file, process_name=''):
        """
        Perform a batch search on the Orbis database using the specified input file.

        - Overview
            - Check if the input file exists, and return if not.
            - Get the name of the input file and use it to set the name of the excel output file.
            - Open a webpage and wait for it to load.
            - Click on some buttons to upload the input file and apply some filters to the search.
            - Wait for the search to complete and then click a button to view the results.
            - Wait for the results page to load and then click on some buttons to customize the displayed data.
            - Extract some data from the page and save it to an excel file.
        :param input_file (_type_): source file to be processed
        :param process_name (_type_): process name to be displayed in the log
        """

        logger.debug(f"Starting batch search for {process_name} ...")

        if not path.exists(input_file):
            logger.debug(
                f"{process_name}: input file {input_file} does not exist")
            return

        excel_output_file_name = path.basename(input_file).split(".")[0]

        self.driver.get(self.orbis_batch_search_url)
        time.sleep(5)

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
        # try:
        #     WebDriverWait(
        #         self.driver,
        #         30 *
        #         60).until(
        #         EC.invisibility_of_element_located(
        #             (By.XPATH,
        #             SEARCH_PROGRESS_BAR)))
        # except Exception as exp:
        #     print(f"Exception is occured while waiting for invisibility of search progress bar")
        try:
            warning_message = self.driver.find_element(
                By.XPATH, SEARCH_PROGRESS_BAR)
            while warning_message.text == "Search is not finished":
                time.sleep(5)
                warning_message = self.driver.find_element(
                    By.XPATH, SEARCH_PROGRESS_BAR)
                logger.debug(f"{process_name}: {warning_message.text}")
                print(f"Search is under progress:\nMessage : {warning_message.text}")
            time.sleep(5)
        except Exception as e:
            logger.debug(
                f"{process_name}: search is not finished: stale element exception {e}")

        self.wait_until_clickable(VIEW_RESULTS_BUTTON)
        view_result_sub_url = self.driver.find_element(
            By.XPATH, VIEW_RESULTS_BUTTON)
        view_result_sub_url.send_keys(Keys.RETURN)

        # this is used to wait until processing overlay is gone
        

        # try:
        #     main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
        #     main_content_style = main_content_div.value_of_css_property(
        #         'max-width')

        #     while main_content_style != "none":
        #         main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
        #         main_content_style = main_content_div.value_of_css_property(
        #             'max-width')
        #         logger.debug(
        #             f"{process_name}main content style is {main_content_style}")
        #         time.sleep(0.5)
        # except Exception as e:
        #     logger.debug(e)

        time.sleep(5)

    
    # wait until the processing overlay is gone

        # check whether the processing overlay is gone
        
        PROCESSING_DIV = "//div[@class='processing-overlay']"

        try: 
            processing_div_main = self.driver.find_element(By.XPATH, PROCESSING_DIV)
            processing_div_main_style = processing_div_main.value_of_css_property('display')
            while processing_div_main_style != "none":
                processing_div_main = self.driver.find_element(By.XPATH, PROCESSING_DIV)
                processing_div_main_style = processing_div_main.value_of_css_property('display')
                logger.debug(f"{process_name} processing div main style is {processing_div_main_style}")
                time.sleep(0.5)
        except Exception as e:
            print(f"{process_name} had an exception: {e} ")
        
        try:
            self.wait_until_clickable(ADD_REMOVE_COLUMNS_VIEW)
            self.driver.find_element(By.XPATH, ADD_REMOVE_COLUMNS_VIEW).click()
        except Exception as e:
            print(f"{process_name} had an exception: {e} ")
            #  add implicit wait
            self.driver.implicitly_wait(10)

        
        try:  
            self.wait_until_clickable(CONTACT_INFORMATION)
            self.driver.find_element(By.XPATH, CONTACT_INFORMATION).click()
        except Exception as e:
            print(f"{process_name} had an exception: {e} ")
        
        try: 
            self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
            search_input = self.driver.find_element(
                By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
            search_input.send_keys("City")
            search_input.send_keys(Keys.RETURN)
            logger.debug(f"{process_name} city is searched")
        except Exception as e:
             print(f"{process_name} had an exception: {e} ")
        
        try:    
            # add city
            self.wait_until_clickable(CITY_COLUMN)
            self.driver.find_element(By.XPATH, CITY_COLUMN).click()
            logger.debug(f"{process_name} city is added")

            self.wait_until_clickable(POPUP_SAVE_BUTTON)
            self.driver.find_element(By.XPATH, POPUP_SAVE_BUTTON).click()
            logger.debug(f"{process_name} popup save button is clicked")
        except Exception as e:
            print(e)

        # self.driver.refresh()
        search_input.clear()
        time.sleep(1)
        try: 
            search_input.send_keys("Country")
            search_input.send_keys(Keys.RETURN)
            logger.debug(f"{process_name} country is searched")
        except Exception as e:
            print(e)
        
        try: 
            self.wait_until_clickable(COUNTRY_COLUMN)
            self.driver.find_element(By.XPATH, COUNTRY_COLUMN).click()
            logger.debug(f"{process_name} country is added")
        except Exception as e:
            print(e)
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
        self.wait_until_clickable(DROPDOWN_APPLY)
        self.driver.find_element(By.XPATH, DROPDOWN_APPLY).click()
        time.sleep(3)

        self.wait_until_clickable(EXCEL_BUTTON)
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
        self.wait_until_clickable(EXCEL_BUTTON)
        self.driver.find_element(By.XPATH, EXPORT_BUTTON).click()
        logger.debug(f"{process_name} export button is clicked")

        self.wait_until_clickable(POPUP_DOWNLOAD_BUTTON)
        while True:
            download_button = self.driver.find_element(
                By.XPATH, POPUP_DOWNLOAD_BUTTON)
            if download_button.value_of_css_property(
                    "background-color") == "rgba(0, 20, 137, 1)":
                # download_button.send_keys(Keys.RETURN)
                # logger.debug(
                #     f"{process_name} waiting for data generation to download ")
                
                time.sleep(0.5)
                print(f"Data is downloaded to {self.data_dir + excel_output_file_name}.xlsx")
                break
            else:
                print(f"waiting for data generation to download ... ")
                time.sleep(2)
        time.sleep(3)
        # ensuring that the file exists otherwise do not continue
        self.check_file_existence(input_file)

    # create a file likewe have data.csv file
    # company name;city;country;identifier
    def generate_data_for_guo(self, orig_orbis_data, file):
        """
        Generates a CSV file containing data in a format that is compatible with the
        requirements of the Guo database. The file will contain the following columns:

        - company name
        - city
        - country
        - identifier

        This method reads an XLSX file located at `orig_orbis_data`, extracts data from
        the 'Results' sheet, and formats it for the Guo database. The resulting CSV file
        will be saved at the location specified by `file`.

        :param orig_orbis_data (str): The path to the original ORBIS data in XLSX format.
        :param file (str): The path to the output CSV file.

        :return:
        None
        """

        if not path.exists(orig_orbis_data):
            logger.debug(f"Original orbis data {orig_orbis_data} is not exists for generating GUO based CSV file, skipping this step !")
            return

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
        """
        Generate data for ISH based on the input ORBIS data and write to a CSV file.

        :param orig_orbis_data (str): The path to the input ORBIS data file in .xlsx format.
        :param file (str): The path to the output CSV file.

        :return:
        None
        """

        if not path.exists(orig_orbis_data):
            logger.debug(f"Original orbis data {orig_orbis_data} is not exists for generating ISH based CSV file, skipping this step !")
            return

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
        """
        Strips new lines from the values of the specified column of a pandas DataFrame.

        :param df (pandas.DataFrame): the DataFrame to modify
        :param colunm_name (str): the name of the column to modify (default is 'Licensee')

        :return:
        pandas.DataFrame: the modified DataFrame
        """

        df[colunm_name] = df[colunm_name].apply(lambda x: x.strip('\n'))
        return df
    
    def check_file_existence(self,file_name):
        if not path.exists(path.join(self.data_dir, file_name)):
            print(f"File {file_name} does not exists, no continuing further !! ")
            sys.exit(1)

    def prepare_data(self, df, df_orbis):
        """
        Prepares the input data for further processing.

        Merges two dataframes, `df` and `df_orbis`, on the 'CIK' column and selects the relevant columns. The resulting
        merged dataframe is then reformatted by renaming columns and dropping irrelevant ones. The method returns the
        resulting dataframe.

        :param df: A pandas dataframe containing license agreement data.
        :param df_orbis: A pandas dataframe containing Orbis data.
        :return: A pandas dataframe containing the merged and prepared data.
        """

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
        related_data['CIK'] = related_data['CIK'].astype(str)
        df_orbis = df_orbis.rename(columns={'Other company ID number': 'CIK'})
        df_orbis['CIK']=df_orbis['CIK'].astype(str)
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
        """
        Saves a pandas dataframe to an excel file.

        :param df: A pandas dataframe to be saved to the excel file.
        :param file_name: The name of the excel file to save the dataframe to.

        :return:
        None
        """

        df.to_excel(file_name)


def run_batch_search(input_file):
    # join path to input file
    """
    Runs batch search using Orbis class for given input file and process name.
    :param input_file(str): File name of the input file to be processed.
    :param process_name(str) : Name of the process.

    :return:
    None
    """

    logger.debug(
        f"Running batch search for file {input_file}")
    with Orbis() as orbis:
        logger.debug(f"Data directory is {orbis.data_dir}")
        logger.debug(f"Input file is {path.join(orbis.data_dir, input_file)}")
        orbis.batch_search(path.join(orbis.data_dir, input_file))

# offline_data_aggregation is used to generate data by considering GUO of companies
# this data needs to be generated when we have new data from Orbis (refer
# workflow in README.md)


def generate_data_for_guo(orbis_data_file):
    """
    Generates data for GUO based on the input Orbis data file and saves the output to a file.

    :param orbis_data_file (str): The name of the input Orbis data file to use. This should be a CSV file containing
            the Orbis data to be processed.
    :param output_file (str): The name of the file to save the output to. This should be a CSV file.

    :return:
    - None: This function does not return anything. The output is saved to the output file instead.


    :raises FileNotFoundError: If the input Orbis data file cannot be found.
    :raises FileNotFoundError: If the output file directory does not exist.
    :raises ValueError: If the output file already exists and the user does not want to overwrite it.

    Notes:
    - This function requires an instance of the Orbis class with the offline flag set to True.
    - The output file will be saved in the same directory as the input Orbis data file.
    - This function will overwrite the output file if it already exists, unless the user specifies otherwise.
    """
    #  output_file name will be generated from the input file name
    
    output_file = f"{path.splitext(orbis_data_file)[0]}_guo.csv"
    
    logger.debug(
        f"Generating data for GUO for file {orbis_data_file} and saving to {output_file}")
    with Orbis(offline=True) as orbis:
        output_file = path.join(orbis.data_dir, output_file)
        orbis_data_file = path.join(orbis.data_dir, orbis_data_file)
        orbis.generate_data_for_guo(
            orig_orbis_data=orbis_data_file,
            file=output_file)


def generate_data_for_ish(orbis_data_file):
    """
    Generates data for ISH from an Orbis Excel file and saves it to a CSV file.


    :param orbis_data_file (str): The name of the input Excel file to read from.

    :return:
    None

    """
    output_file = f"{path.splitext(orbis_data_file)[0]}_ish.csv"
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
    """
    Aggregates data from an Orbis file and saves the aggregated output to a file.


    :param orbis_file (str): The name of the Orbis file to aggregate data from.
    :param aggregated_output_file (str): The name of the file to save the aggregated data to.

    :return:
    None: This function doesn't return anything.
    """
    logger.debug(
        f"Aggregating data for file {orbis_file} and saving to {aggregated_output_file}")
   
    with Orbis(offline=True) as orbis:
        # data processing and augmentation
        # after orbis search step following needs to run
        orbis_file = path.join(orbis.data_dir, orbis_file)
        aggregate_output_file = path.join(
            orbis.data_dir, aggregated_output_file)

        if not path.exists(orbis.license_data):
            logger.debug(f"License file, {orbis.license_data}, does not exists, skipping to aggreagate data !")
            return 

        if not path.exists(orbis.license_data):
            logger.debug(f"Orbis file, {orbis_file}, does not exists, skipping to aggreagate data !")
            return 
        
        df_licensee_data = orbis.read_xlxs_file(orbis.license_data, sheet_name='')
        df_orbis_data = orbis.read_xlxs_file(orbis_file, sheet_name='')
        df_licensee = orbis.strip_new_lines(df_licensee_data)
        df_result = orbis.prepare_data(df_licensee, df_orbis_data)
        orbis.to_xlsx(df_result, aggregate_output_file)
        logger.debug(
            f"Data is aggregated and saved to {aggregate_output_file}")


def generate_unique_id(company_name, n):
    """
    Generate a unique ID for a company using SHA-256 hashing.


    :param company_name (str): The name of the company.
    :param n (int): The number of characters to include in the unique ID.

    :return:
    str: A unique ID string with n characters.
    """
    logger.debug(f"Generating unique id for {company_name}")
    sha256 = hashlib.sha256()
    sha256.update(company_name.encode())
    return sha256.hexdigest()[:n]


def post_process_data(excel_file):
    # create another column for unique identifier with hash for column 1 however no negative values
    # drop all rows where Orbis ID number is null
    # get data/ folder path and append file name to it
    """
    Post-processes the data by creating a column for unique identifier with hash for column 1.
    Drops all rows where Orbis ID number is null.
    Gets data/ folder path and appends file name to it.


    :param excel_file (str): File path of the excel file to post-process.

    :return:
    None
    """
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
    df = pd.read_excel(excel_file, sheet_name='Sheet1')
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


def run_in_parallel_generic(function, args):
    """
    Runs a given function in parallel using multiple processes. It uses the Python multiprocessing module to create a process pool, and distributes the function calls across the available CPUs.


    :param function (function): The function to be run in parallel.
    :param starmap (bool): If True, use starmap() function to apply the function to the input tuples in args. Otherwise, use imap() function.
    :param args (list): A list of tuples, where each tuple contains the arguments to be passed to the function.

    :return:
    None

    :raises:
    None
    """

    # number_of_processes = multiprocessing.cpu_count()
    # if starmap:
        
    #   with ThreadPoolExecutor(max_workers=number_of_processes) as executor:
    #     # if starmap:
    #     #     executor.starmap(function, args)
    #     # else:
    #     #     executor.map(function, args)
    #    future = executor.submit(function, args)
    #    print(future.result())
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(function, file_to_be_processed): file_to_be_processed for file_to_be_processed in args}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                print(f"result is --->  {data}")
        
def save_screenshot(driver, file_name):
    """
    Save a screenshot of the current browser window to the specified file.

    :param driver: Selenium WebDriver instance to use for taking the screenshot
    :type driver: selenium.webdriver.remote.webdriver.WebDriver
    :param file_name: File name and path to save the screenshot to
    :type file_name: str
    """
    driver.save_screenshot(f'{file_name}.png')
    logger.debug(f"Screenshot saved to {file_name}.png")


if __name__ == "__main__":

    # initial checks
    if environ.get('LOCAL_DEV') == 'True':
        environ["DATA_SOURCE"] = "sample_data.xlsx"
        if not path.exists(environ.get('CONFIG_PATH')):
            # exit with an error message
            exit(
                f"Config file {path.abspath(environ.get('CONFIG_PATH')) } does not exist")

    # start to work

    timestamp = datetime.now().strftime("%d_%m_%Y")
    # Step 1
    # --> crawl_data.py should generate data in data/data.csv
    
    # crawl_data: prepare_data
    # generates csv file for licensee
    files_to_apply_batch_search = [f"orbis_data_licensee_{timestamp}.csv",
                             f"orbis_data_licensor_{timestamp}.csv",
                          
                           ]

    generate_input_file_for_guo = [f"orbis_data_licensee_{timestamp}.xlsx",
                             f"orbis_data_licensor_{timestamp}.xlsx",
                            ]

    generate_input_file_for_ish = [f"orbis_data_licensee_{timestamp}.xlsx",
                              f"orbis_data_licensor_{timestamp}.xlsx",
                             ]

    create_input_file_for_orbis_batch_search(
        environ.get("DATA_SOURCE"), 
        f"orbis_data_licensee_{timestamp}.csv",
        is_licensee=True)

    # generates csv file for licensor
    create_input_file_for_orbis_batch_search(
        environ.get("DATA_SOURCE"),
        f"orbis_data_licensor_{timestamp}.csv",
        is_licensee=False)

    time.sleep(4)  # wait for 4 seconds for data to be saved in data folder

    # # Step 2
    # # --> data/data.csv needs to be uploaded to Orbis to start batch search
    # run_batch_search(config_path, f"orbis_d.csv") # Todo: this csv file
    # needs to come from crawl_data.py
    # run_in_parallel_generic(function=run_batch_search,
    #                         args=[f"orbis_data_licensee_{timestamp}.csv",
    #                          f"orbis_data_licensor_{timestamp}.csv"])

    # run_batch_search(config_path, f"orbis_data_{timestamp}.csv") # Todo: this csv file needs to come from crawl_data.py
    # # # # --> after batch search is completed, data downloaded from Orbis
    for file_to_search in files_to_apply_batch_search:
        print(f"Now searching for file {file_to_search}")
        run_batch_search(file_to_search)
        time.sleep(5)
        print(f"Search is done for file {file_to_search}")

     # # # # Step 3
    # # # # --> generate_data_for_guo to generate data by considering GUO of companies

    run_in_parallel_generic(function=generate_data_for_guo,
                            args=[f"orbis_data_licensee_{timestamp}.xlsx",
                             f"orbis_data_licensor_{timestamp}.xlsx",
                            ])


    input_files_with_guo_info = [f"orbis_data_licensee_{timestamp}_guo.csv",
                             f"orbis_data_licensor_{timestamp}_guo.csv"]
                             
    for guo_input_file in input_files_with_guo_info:
        print(f"Running for the file with guo info: {guo_input_file}")
        run_batch_search(guo_input_file)
        time.sleep(4)
        print(f"Search is done for guo file {guo_input_file}")
        


    # # # Step 3
    # # # --> generate_data_for_guo to generate data by considering GUO of companies
        
    # # # Step 3
    # # # --> generate_data_for_guo to generate data by considering GUO of companies


    input_files_with_ish_info =   [f"orbis_data_licensee_{timestamp}_ish.csv",
                             f"orbis_data_licensor_{timestamp}_ish.csv"]
                             
    time.sleep(2)  # wait for 2 seconds for data to be saved in data folder

    run_in_parallel_generic(function=generate_data_for_ish,
                            args=[f"orbis_data_licensee_{timestamp}.xlsx",
                              f"orbis_data_licensor_{timestamp}.xlsx",
                             ])

    
    for ish_input_file in input_files_with_ish_info:
        print(f"Running for the file with guo info: {ish_input_file}")
        run_batch_search(ish_input_file)
        time.sleep(4)
        print(f"Search is done for guo file {ish_input_file}")

    # run_in_parallel_generic(function=run_batch_search,
    #                         args=[f"orbis_data_licensee_{timestamp}_guo.csv",
    #                          f"orbis_data_licensor_{timestamp}_guo.csv",
    #                          ])

    # time.sleep(2)  # wait for 2 seconds for data to be saved in data folder

    # run_in_parallel_generic(function=run_batch_search,
    #                         args= [f"orbis_data_licensee_ish_{timestamp}.csv",
    #                          f"orbis_data_licensor_ish_{timestamp}.csv",
    #                          ])
    # # # # # # Step 5
    time.sleep(2)  # wait for 2 seconds for data to be saved in data folder

    try: 
        aggregate_data(f"orbis_data_licensee_{timestamp}.xlsx",f"orbis_aggregated_data_licensee_{timestamp}.xlsx")
    except FileNotFoundError as fne:
        print(f"File not found in aggregating the data: excp: {fne}. Please make sure it exists !")

    try:     
        aggregate_data(f"orbis_data_licensor_{timestamp}.xlsx",f"orbis_aggregated_data_licensor_{timestamp}.xlsx")
    except FileNotFoundError as fne: 
        print(f"File could not be found to aggregate please make sure it exists !")
    # run_in_parallel_generic(function=aggregate_data,
    #                         [(f"orbis_data_licensee_{timestamp}.xlsx",
    #                           f"orbis_aggregated_data_licensee_{timestamp}.xlsx"),
    #                          (f"orbis_data_licensor_{timestamp}.xlsx",
    #                           f"orbis_aggregated_data_licensor_{timestamp}.xlsx")])

    run_in_parallel_generic(function=post_process_data,
                            args=[f"orbis_aggregated_data_{timestamp}.xlsx",
                                f"orbis_aggregated_data_licensee_{timestamp}.xlsx",
                                f"orbis_aggregated_data_licensor_{timestamp}.xlsx",
                                f"orbis_aggregated_data_guo_{timestamp}.xlsx",
                                f"orbis_data_guo_{timestamp}.xlsx",
                                f"orbis_data_licensee_{timestamp}_ish.xlsx",
                                f"orbis_data_licensor_{timestamp}_ish.xlsx",
                                f"orbis_data_licensee_{timestamp}.xlsx",
                                f"orbis_data_licensee_{timestamp}_guo.xlsx",
                                f"orbis_data_licensor_{timestamp}.xlsx",
                                f"orbis_data_licensor_{timestamp}_guo.xlsx",
                                f"orbis_data_{timestamp}.xlsx"])
