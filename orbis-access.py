# Description: This script applies batch search on the Orbis website
# Python Version: 3.9.5
# Python Libraries: selenium, pandas, yaml

import re
import yaml 
import time
import pandas as pd
import numpy as np 
import multiprocessing
import multiprocessing.pool
from os import path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from argparse import ArgumentParser
from datetime import datetime
from crawl_data import prepare_data


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
# PL_FOR_PROFIT_LOSS_SETTINGS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.PL:IND"]/div[1]/span[2]'
CASH_FLOW_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.CF:UNIVERSAL"]/div[1]'
TOTAL_ASSETS_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.TOAS:UNIVERSAL"]/div[1]'
NUMBER_OF_EMPLOYEES_SETTINGS = '/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[3]/div/ul/li[2]/div[1]'
OPERATING_PL_SETTINS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.OPPL:IND"]/div[1]'
GROSS_PROFIT  = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.GROS:IND"]/div[1]'
SALES_SETTINGS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.TURN:IND"]/div[1]'
ABSOLUTE_IN_COLUMN_OP = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[1]/div/table/tbody/tr/td[2]/a'

SCROLLABLE_XPATH =  '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div'
SCROLLABLE_XPATH_IN_SECOND_OPTION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[3]/div/div'

ANNUAL_DATA_LIST = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div/ul'
MILLION_UNITS = '//*[@id="ClassicOption"]/div/div[3]/div[1]/ul/li[3]/label'
OP_REVENUE_OK_BUTTON = '/html/body/section[2]/div[6]/div[3]/a[2]'
EXCEL_EXPORT_NAME_FIELD = '//*[@id="component_FileName"]'
MAIN_DIV = '//*[@id="main-content"]'

SEARCH_INPUT_ADD_RM_COLUMNS = '//*[@id="Search"]'
SEARCH_ICON_ADD_RM_COLUMNS = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[1]/div[2]/div/span'
CIK_NUMBER_VIEW= '//*[@id="IDENTIFIERS*IDENTIFIERS.COMPANY_ID_NUMBER:UNIVERSAL"]/div[2]/span' # Other company ID number
POPUP_SAVE_BUTTON= '/html/body/section[2]/div[6]/div[3]/a[2]' # when adding Other company ID number column to data view
CITY_COLUMN= '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.CITY:UNIVERSAL"]/div[2]/span'
COUNTRY_COLUMN= '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.COUNTRY:UNIVERSAL"]/div[2]/span'
CONTACT_INFORMATION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[3]/div'
BVD_SECTORS = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.BVD_SECTOR_CORE_LABEL:UNIVERSAL"]/div[2]/span'
US_SIC_PRIMARY_CODES = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_PRIMARY_CODE:UNIVERSAL"]/div[2]/span'
US_SIC_SECONDARY_CODES = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_SECONDARY_CODE:UNIVERSAL"]/div[2]/span'


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


class Orbis:
    

    def __init__(self, config_path, offline=False):
        config = self.read_config(config_path)
        self.executable_path = config["selenium"]["executable_path"]
        self.orbis_access_url = config["orbis"]["urls"]["access"]
        self.orbis_batch_search_url = config["orbis"]["urls"]["batch"]
        self.orbis_logout_url = config["orbis"]["urls"]["logout"]
        self.email_address = config["orbis"]["email"]
        self.password = config["orbis"]["password"]
        self.driver_options = config["selenium"]["driver_options"]
        self.data_dir = config["data"]["path"]
        self.data_path = path.join(self.data_dir, config["data"]["file"])
        self.driver = None
        self.offline = offline
        self.license_data = config["data"]["path"] + config["data"]["license"]
        self.orbis_data = config["data"]["path"] + config["data"]["orbis"]
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
            "US SIC, primary code(s)": US_SIC_PRIMARY_CODES,
            "US SIC, secondary code(s)": US_SIC_SECONDARY_CODES,
        }
        
    def __exit__(self, exc_type, exc_value, traceback):
        
        if not self.offline:    
            self.logout()
            if self.driver is not None:
                return self.driver.close()
        return None
    
    def __enter__(self):
        if not self.offline:
            self.chrome_options = webdriver.ChromeOptions()
            prefs = {'download.default_directory' : self.data_dir}
            self.chrome_options.add_experimental_option('prefs', prefs)
            self.chrome_options.add_experimental_option("detach", True)
            self.driver = webdriver.Chrome(executable_path=self.executable_path, chrome_options=self.chrome_options)
            self.login()
        return self
    
        
    def get_financial_columns(self):
        financial_columns = list(self.variables.keys())
        financial_columns = financial_columns[:len(financial_columns)-2] # remove the last two columns
        return financial_columns
    
    
    def drop_columns(self, df, regex):
       columns_to_drop = df.filter(regex=regex).columns
       print("Dropping columns: ", columns_to_drop)
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
        password = self.driver.find_element(By.ID,"pass")
        # fill the fields with given values
        username.send_keys(self.email_address)
        password.send_keys(self.password)
        # find with XPath the login button and click it
        login_button = self.driver.find_element(By.XPATH,LOGIN_BUTTON)
        # click the login button
        login_button.click()

    def logout(self):
        # logout from the web site
        time.sleep(3)
        self.driver.get(self.orbis_logout_url)
        
    def scroll_to_bottom(self):
        # scroll to the bottom of the page
        # only used after add/remove column step
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        
        
    def wait_until_clickable(self, xpath):
        # wait until the element is clickable
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        
    def read_xlxs_file(self, file_path, sheet_name=''):
        if sheet_name == '': 
            df = pd.read_excel(file_path)
        else: 
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    
    
    def check_checkboxes(self, field=''):
        try: 
            
            scrollable = self.driver.find_element(By.XPATH, SCROLLABLE_XPATH)
            scroll_position = self.driver.execute_script("return arguments[0].scrollTop", scrollable)
            # print(f"scroll position is {scroll_position}")
            height_of_scrollable = self.driver.execute_script("return arguments[0].scrollHeight", scrollable)
            # print(f"height of scrollable is {height_of_scrollable}")    
            scroll_amount = 100
            while scroll_position < height_of_scrollable:
                scrollable = self.driver.find_element(By.XPATH, SCROLLABLE_XPATH)
                checkboxes = scrollable.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
                for checkbox in checkboxes:
                    try: 
                        if checkbox.get_attribute("checked") == "true":
                            continue
                        checkbox.click()
                    except Exception as e:
                        # print(f"exception on checkbox {e}")
                        continue        
                self.driver.execute_script(f"arguments[0].scrollTo(0,{scroll_position})", scrollable)
                scroll_position += scroll_amount
                print (f"{field} scroll position is {scroll_position}")
                time.sleep(0.5)
        except Exception as e:
            print(e)
    
    # select_all_years function is used to get the operating revenue data in millions for all available years
    def select_all_years(self, field='', is_financial_data=True, is_checked=False):
        # click to finanacial data column        
        if not is_checked: 
            
            self.wait_until_clickable(ABSOLUTE_IN_COLUMN_OP)
            self.driver.find_element(By.XPATH, ABSOLUTE_IN_COLUMN_OP).click()

            self.wait_until_clickable(ANNUAL_DATA_LIST)
            
            self.check_checkboxes(field)

            try:
                if is_financial_data:
                    self.wait_until_clickable(MILLION_UNITS)
                    self.driver.find_element(By.XPATH, MILLION_UNITS).click()
            except Exception as e:
                print(e)
            
            self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
            self.driver.find_element(By.XPATH, OP_REVENUE_OK_BUTTON).click()     
        
        elif not is_financial_data:
            self.check_checkboxes(field)
            
            self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
            self.driver.find_element(By.XPATH, OP_REVENUE_OK_BUTTON).click()        
        

    def batch_search(self, input_file):
        
        if not path.exists(input_file):
            print(f"input file {input_file} does not exist")
            return

        excel_output_file_name = path.basename(input_file).split(".")[0] 
        
        self.driver.get(self.orbis_batch_search_url)
        
        self.wait_until_clickable(DRAG_DROP_BUTTON)
        self.driver.find_element(By.XPATH, DRAG_DROP_BUTTON).click()

        
        file_input=self.driver.find_element(By.XPATH, SELECT_FILE_BUTTON)
        file_input.send_keys(input_file)
        
        self.wait_until_clickable(UPLOAD_BUTTON)

        self.driver.find_element(By.XPATH, UPLOAD_BUTTON).click()
    
        self.wait_until_clickable(FIELD_SEPERATOR)

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR  ).clear()
        time.sleep(2)

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).send_keys(";")

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).send_keys(Keys.RETURN)
        
        print("field seperator is cleared")
        time.sleep(2)
        self.wait_until_clickable(APPLY_BUTTON)

        self.driver.find_element(By.XPATH, APPLY_BUTTON).click()
        
        time.sleep(5)
        WebDriverWait(self.driver, 30*60).until(EC.invisibility_of_element_located((By.XPATH, SEARCH_PROGRESS_BAR)))

        
        warning_message = self.driver.find_element(By.XPATH, SEARCH_PROGRESS_BAR)
        while warning_message.text == "Search is not finished":
                time.sleep(5)
                warning_message = self.driver.find_element(By.XPATH, SEARCH_PROGRESS_BAR)
                print(warning_message.text)
        
        
        self.wait_until_clickable(VIEW_RESULTS_BUTTON)
        
        view_result_sub_url = self.driver.find_element(By.XPATH, VIEW_RESULTS_BUTTON)
        view_result_sub_url.send_keys(Keys.RETURN)
       
        # this is used to wait until processing overlay is gone
        try:
            main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
            main_content_style = main_content_div.value_of_css_property('max-width')
            
            while main_content_style != "none":
                main_content_div = self.driver.find_element(By.XPATH, MAIN_DIV)
                main_content_style = main_content_div.value_of_css_property('max-width')
                print(f"main content style is {main_content_style}")
                time.sleep(0.5)
        except Exception as e:
            print(e)
            
                    
        self.wait_until_clickable(ADD_REMOVE_COLUMNS_VIEW)    
        self.driver.find_element(By.XPATH, ADD_REMOVE_COLUMNS_VIEW).click()
       
        
        self.wait_until_clickable(CONTACT_INFORMATION)
        self.driver.find_element(By.XPATH, CONTACT_INFORMATION).click()
        
        
        
        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input = self.driver.find_element(By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.send_keys("City")
        search_input.send_keys(Keys.RETURN)
        
        # add city 
        self.wait_until_clickable(CITY_COLUMN)
        self.driver.find_element(By.XPATH, CITY_COLUMN).click()
        
        
        self.wait_until_clickable(POPUP_SAVE_BUTTON)
        self.driver.find_element(By.XPATH, POPUP_SAVE_BUTTON).click()
        
        # self.driver.refresh()
        search_input.clear()
        time.sleep(1)
        
        search_input.send_keys("Country")
        search_input.send_keys(Keys.RETURN)
        
        self.wait_until_clickable(COUNTRY_COLUMN)
        self.driver.find_element(By.XPATH, COUNTRY_COLUMN).click()
        
            
        search_input.clear()
        time.sleep(1) 

        # identification number column 
        
        self.wait_until_clickable(IDENTIFICATION_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, IDENTIFICATION_NUMBER_VIEW).click()
        
        # search input for Other company ID number (CIK number)
        self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
        # try: 
        search_input = self.driver.find_element(By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
        search_input.send_keys("Other company ID number")
        search_input.send_keys(Keys.RETURN)
        # except Exception as e:
        #     print(e)
                
        self.wait_until_clickable(CIK_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, CIK_NUMBER_VIEW).click()
        
        
        self.wait_until_clickable(POPUP_SAVE_BUTTON)
        self.driver.find_element(By.XPATH, POPUP_SAVE_BUTTON).click()
        
        # self.driver.refresh()
        search_input.clear()
        time.sleep(1)
        self.wait_until_clickable(IDENTIFICATION_NUMBER_VIEW)
        self.driver.find_element(By.XPATH, IDENTIFICATION_NUMBER_VIEW).click()
        
        # add BVD ID number column
       
        self.wait_until_clickable(BVD_ID_NUMBER_ADD)
        self.driver.find_element(By.XPATH, BVD_ID_NUMBER_ADD).click()
        
        # add ORBIS ID number column
       
        self.wait_until_clickable(ORBIS_ID_NUMBER_ADD)
        self.driver.find_element(By.XPATH, ORBIS_ID_NUMBER_ADD).click()
        

        time.sleep(2)
        # scroll down within in panel 
        self.scroll_to_bottom()
        time.sleep(1)
        
        # Ownership Data > //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div
       
        self.wait_until_clickable(OWNERSHIP_COLUMN)
        self.driver.find_element(By.XPATH, OWNERSHIP_COLUMN).click()
        
        
        # scroll down within in panel 
        self.scroll_to_bottom()
        time.sleep(1)
        
        # Shareholders //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div
        
        self.wait_until_clickable(SHAREHOLDERS_COLUMN)
        self.driver.find_element(By.XPATH, SHAREHOLDERS_COLUMN).click()
       
        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Information 
        
        self.wait_until_clickable(GUO_COLUMN)
        self.driver.find_element(By.XPATH, GUO_COLUMN).click()
       
        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Name //*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span
        
        self.wait_until_clickable(GUO_NAME_INFO)
        self.driver.find_element(By.XPATH, GUO_NAME_INFO).click()
        
        self.scroll_to_bottom()
        time.sleep(1)
        
        self.wait_until_clickable(IMMEDIATE_PARENT_COMPANY_NAME)
        self.driver.find_element(By.XPATH, IMMEDIATE_PARENT_COMPANY_NAME).click()
        
            
        self.wait_until_clickable(ISH_NAME)
        self.driver.find_element(By.XPATH, ISH_NAME).click()
        
        non_financial_items = list(set(self.variables.keys())-set(self.get_financial_columns()))
        is_checked = False 
        for item, xpath in self.variables.items():
            
            self.wait_until_clickable(SEARCH_INPUT_ADD_RM_COLUMNS)
            search_input = self.driver.find_element(By.XPATH, SEARCH_INPUT_ADD_RM_COLUMNS)
            search_input.clear()
            search_input.send_keys(item)
            search_input.send_keys(Keys.RETURN)
            time.sleep(2)
            self.wait_until_clickable(xpath)
            self.driver.find_element(By.XPATH, xpath).click()
            time.sleep(1)
            if item in non_financial_items:
                self.select_all_years(field = item, is_financial_data=False, is_checked=is_checked)
            else:
                self.select_all_years(field = item, is_checked=is_checked)
            # self.driver.refresh()
            print(f"item : {item} xpath : {xpath} ")
            is_checked = True
        # apply changes button 
        self.wait_until_clickable(APPLY_CHANGES_BUTTON)
        self.driver.find_element(By.XPATH, APPLY_CHANGES_BUTTON).click()
        
        # currency select unit 
        self.wait_until_clickable('/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[4]/a')
        self.driver.find_element(By.XPATH, '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[4]/a').click()
        
        # select million
        self.wait_until_clickable('//*[@id="id-currency-menu-popup"]/ul[1]/li[4]')
        self.driver.find_element(By.XPATH, '//*[@id="id-currency-menu-popup"]/ul[1]/li[4]').click()
        
        
        # click apply from dropdown
        
        self.wait_until_clickable('//*[@id="id-currency-menu-popup"]/div/a[2]')
        self.driver.find_element(By.XPATH, '//*[@id="id-currency-menu-popup"]/div/a[2]').click()
        time.sleep(2)
        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXCEL_BUTTON), 'Excel'))
        self.driver.find_element(By.XPATH, EXCEL_BUTTON).click()
        
        self.wait_until_clickable(EXCEL_EXPORT_NAME_FIELD)
        excel_filename_input = self.driver.find_element(By.XPATH, EXCEL_EXPORT_NAME_FIELD)
        excel_filename_input.clear()
        excel_filename_input.send_keys(excel_output_file_name)
        # excel_output_file_name.send_keys(Keys.RETURN)
        time.sleep(2)
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXPORT_BUTTON), 'Export'))
        self.driver.find_element(By.XPATH, EXPORT_BUTTON).click()
        
                
        self.wait_until_clickable(POPUP_DOWNLOAD_BUTTON)
        while True:
            download_button= self.driver.find_element(By.XPATH, POPUP_DOWNLOAD_BUTTON)
            if download_button.value_of_css_property("background-color") == "rgba(0, 20, 137, 1)":
                # download_button.send_keys(Keys.RETURN)       
                time.sleep(0.5)         
                break
            else: 
                time.sleep(2)

    # create a file likewe have data.csv file
    # company name;city;country;identifier
    def generate_data_for_guo(self,orig_orbis_data, file):
        df = self.read_xlxs_file(orig_orbis_data,sheet_name='Results')
        df = df[['GUO - Name','City\nLatin Alphabet','Country','Other company ID number']]
        # drop rows where company name is null
        df = df[df['GUO - Name'].notna()]
        df.to_csv(file, index=False,header=['company name','city','country','identifier'], sep=';')
        
        
    
    def strip_new_lines(self, df, colunm_name='Licensee'):
        df[colunm_name] = df[colunm_name].apply(lambda x: x.strip('\n'))
        return df 
    
    def prepare_data(self, df, df_orbis):
        related_data = df[['Licensee','Licensee CIK 1_cleaned','Licensor','Agreement Date']]
        # drop rows where Licensee CIK 1_cleaned is null
        related_data = related_data[related_data['Licensee CIK 1_cleaned'].notna()]
        
        
        related_data['Agreement Date'] = pd.to_datetime(related_data['Agreement Date'])
        related_data['Financial Period'] = related_data['Agreement Date'].dt.year - 1
        related_data = related_data.rename(columns={'Licensee CIK 1_cleaned': 'CIK'})
        df_orbis = df_orbis.rename(columns={'Other company ID number': 'CIK'})
        df_merged = df_orbis.merge(related_data, on='CIK')

        df_merged = self.drop_columns(df_merged, 'Last avail. yr$')  # drop columns which ends with Last avail. yr
        for index, r in df_merged.iterrows():
            year = r['Financial Period']
            number_of_employees = r[f'Number of employees\n{year}']
            df_merged.at[index, 'Number of employees (in Financial Period)'] = number_of_employees
            try: 
                for f_colunm in self.get_financial_columns():
                    revenue_info = r[f'{f_colunm}\nm USD {year}']
                    df_merged.at[index, f'{f_colunm}\nm USD'] = revenue_info
                   
            except KeyError as ke: 
                print(ke)
   
        df_merged = self.drop_columns(df_merged, '[0-9]$') # drop columns which ends with a year 
       
        return df_merged
    
    def to_xlsx(self, df, file_name):
        df.to_excel(file_name)
        

def run_batch_search(config_path, input_file):
    with Orbis(config_path) as orbis:
        orbis.batch_search(orbis.data_dir+input_file)

# offline_data_aggregation is used to generate data by considering GUO of companies
# this data needs to be generated when we have new data from Orbis (refer workflow in README.md)
def generate_data_for_guo(config_path, orbis_data_file, output_file):
    print(f"Generating data for GUO for file {orbis_data_file} and saving to {output_file}")
    with Orbis(config_path, offline=True) as orbis:
        output_file = path.join(orbis.data_dir, output_file)
        orbis_data_file = path.join(orbis.data_dir, orbis_data_file)
        orbis.generate_data_for_guo(orig_orbis_data=orbis_data_file, file=output_file)



# aggregate_data is used to aggregate data by considering Licensee of companies
# (refer workflow in README.md) 
def aggregate_data(config_path, orbis_file, aggregated_output_file):
    with Orbis(config_path, offline=True) as orbis:
        # data processing and augmentation
        # after orbis search step following needs to run 
        orbis_file = path.join(orbis.data_dir, orbis_file)
        aggregate_output_file = path.join(orbis.data_dir, aggregated_output_file)
        df_licensee_data = orbis.read_xlxs_file(orbis.license_data)
        df_orbis_data = orbis.read_xlxs_file(orbis_file, sheet_name='Results')
        df_licensee = orbis.strip_new_lines(df_licensee_data)
        df_result = orbis.prepare_data(df_licensee, df_orbis_data)
        orbis.to_xlsx(df_result, aggregate_output_file)

def run_batch_in_parallel(config_path):
    with Orbis(config_path, offline=True) as orbis:
        with multiprocessing.pool.ThreadPool(2) as pool:
            results = pool.starmap(run_batch_search, [(config_path, orbis.data_path), (config_path, orbis.data_dir+'guo_data.csv')])

def run_in_parallel_generic(function, args):
    with multiprocessing.pool.ThreadPool(2) as pool:
        results = pool.starmap(function, args)
    
if __name__ == "__main__":
    config_path = "./config/config.yaml"
    timestamp = datetime.now().strftime("%d_%m_%Y")

    # Step 1 
    # --> crawl_data.py should generate data in data/data.csv
    # prepare_data(config_path, "sample_data.xlsx",f"orbis_data_{timestamp}.csv")
    
    # # Step 2 
    # # --> data/data.csv needs to be uploaded to Orbis to start batch search
    # run_batch_search(config_path, f"orbis_d.csv") # Todo: this csv file needs to come from crawl_data.py

    # run_batch_search(config_path, f"orbis_data_{timestamp}.csv") # Todo: this csv file needs to come from crawl_data.py
    # # # # --> after batch search is completed, data downloaded from Orbis
    
    # time.sleep(2) # wait for 2 seconds for data to be saved in data folder
    
    # # # # Step 3 
    # # # # --> generate_data_for_guo to generate data by considering GUO of companies
    # generate_data_for_guo(config_path, orbis_data_file=f"orbis_data_{timestamp}.xlsx", output_file=f"orbis_data_guo_{timestamp}.csv")

    # time.sleep(2) # wait for 1 second for data to be saved in data folder
    
    # # # # Step 4
    # # # # # --> run batch search for guo_data
    # run_batch_search(config_path, f"orbis_data_guo_{timestamp}.csv")
    
    # # # # # Step 5
    # time.sleep(2) # wait for 2 seconds for data to be saved in data folder
    
    # # # # --> aggregate_data to aggregate data by considering Licensee of companies
    # aggregate_data(config_path, f"orbis_data_{timestamp}.xlsx", f"orbis_aggregated_data_{timestamp}.xlsx")  #  aggregate data by considering the file searched with data.csv
    # aggregate_data(config_path, f"orbis_data_guo_{timestamp}.xlsx", f"orbis_aggregated_data_guo_{timestamp}.xlsx")  #  aggregate data by considering the file searched with guo_data.csv
    aggregate_data(config_path, f"orbis_d.xlsx", f"orbis_aggregated_d.xlsx")  #  aggregate data by considering the file searched with data.csv

    # run_in_parallel_generic(function = aggregate_data, args=[(config_path, f"orbis_data_{timestamp}.xlsx", f"orbis_aggregated_data_{timestamp}.xlsx"), (config_path, f"orbis_data_guo_{timestamp}.xlsx", f"orbis_aggregated_data_guo_{timestamp}.xlsx")])