# Description: This script applies batch search on the Orbis website
# Python Version: 3.9.5
# Python Libraries: selenium, pandas, yaml

import re
import yaml 
import time
import pandas as pd
import numpy as np 
from os import path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from argparse import ArgumentParser
from datetime import datetime



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
ABSOLUTE_IN_COLUMN_OP = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[1]/div/table/tbody/tr/td[2]/a'

SCROLLABLE_XPATH =  '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div'
SCROLLABLE_XPATH_IN_SECOND_OPTION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[3]/div/div'

ANNUAL_DATA_LIST = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div/ul'
MILLION_UNITS = '//*[@id="ClassicOption"]/div/div[3]/div[1]/ul/li[3]/label'
OP_REVENUE_OK_BUTTON = '/html/body/section[2]/div[6]/div[3]/a[2]'

SEARCH_INPUT_ADD_RM_COLUMNS = '//*[@id="Search"]'
SEARCH_ICON_ADD_RM_COLUMNS = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[1]/div[2]/div/span'
CIK_NUMBER_VIEW= '//*[@id="IDENTIFIERS*IDENTIFIERS.COMPANY_ID_NUMBER:UNIVERSAL"]/div[2]/span' # Other company ID number
POPUP_SAVE_BUTTON= '/html/body/section[2]/div[6]/div[3]/a[2]' # when adding Other company ID number column to data view

# IDENTIFICATION_NUMBER_VIEW = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]'
IDENTIFICATION_NUMBER_VIEW = '/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]/div'


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
    

    def __init__(self, config_path):
        config = self.read_config(config_path)
        self.executable_path = config["selenium"]["executable_path"]
        self.orbis_access_url = config["orbis"]["urls"]["access"]
        self.orbis_batch_search_url = config["orbis"]["urls"]["batch"]
        self.orbis_logout_url = config["orbis"]["urls"]["logout"]
        self.email_address = config["orbis"]["email"]
        self.password = config["orbis"]["password"]
        self.driver_options = config["selenium"]["driver_options"]
        self.data_path = path.join(config["data"]["path"], config["data"]["file"])
        self.driver = None
        self.license_data = config["data"]["path"] + config["data"]["license"]
        self.orbis_data = config["data"]["path"] + config["data"]["orbis"]
        
        
    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver is not None:
            return self.driver.close()
        return None
    
    def __enter__(self):
        return self
    
    def init_driver(self):
        self.chrome_options = webdriver.ChromeOptions()
            # for options in self.driver_options:
            #     self.chrome_options.add_argument(options)
        # prefs = {"download.default_directory":self.data_path}
        # self.chrome_options.add_experimental_option("prefs",prefs)
        self.chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(executable_path=self.executable_path, chrome_options=self.chrome_options)
        
        
        
        
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
    
    
    # get_op_revenue_data function is used to get the operating revenue data in millions for all available years
    def get_op_revenue_data(self):
        # click to finanacial data column
        self.wait_until_clickable(FINANCIAL_DATA_BUTTON)
        self.driver.find_element(By.XPATH, FINANCIAL_DATA_BUTTON).click()
        
        self.wait_until_clickable(KEY_FINANCIAL_DATA)
        self.driver.find_element(By.XPATH, KEY_FINANCIAL_DATA).click()

        self.wait_until_clickable(OP_REVENUE_SETTINGS)
        self.driver.find_element(By.XPATH, OP_REVENUE_SETTINGS).click()
        
        self.wait_until_clickable(ABSOLUTE_IN_COLUMN_OP)
        self.driver.find_element(By.XPATH, ABSOLUTE_IN_COLUMN_OP).click()
        
        self.wait_until_clickable(ANNUAL_DATA_LIST)
        
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
                    except:
                        continue
                                
                self.driver.execute_script(f"arguments[0].scrollTo(0,{scroll_position})", scrollable)
                scroll_position += scroll_amount
                print (f"operating revenue scroll position is {scroll_position}")
                # time.sleep(0.5)
        except Exception as e:
            print(e)
             
        self.wait_until_clickable(MILLION_UNITS)
        self.driver.find_element(By.XPATH, MILLION_UNITS).click()
        
        self.wait_until_clickable(OP_REVENUE_OK_BUTTON)
        self.driver.find_element(By.XPATH, OP_REVENUE_OK_BUTTON).click()        
    

    def batch_search(self):
        
        self.driver.get(self.orbis_batch_search_url)
        
        self.wait_until_clickable(DRAG_DROP_BUTTON)
        self.driver.find_element(By.XPATH, DRAG_DROP_BUTTON).click()

        
        file_input=self.driver.find_element(By.XPATH, SELECT_FILE_BUTTON)
        file_input.send_keys(self.data_path)
        
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
       
        # add/remove columns //*[@id="main-content"]/div/div[2]/div[1]/a
        self.wait_until_clickable(ADD_REMOVE_COLUMNS_VIEW)
        self.driver.find_element(By.XPATH, ADD_REMOVE_COLUMNS_VIEW).click()
        
        
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
        
         # add operating revenue column in millions USD for all available years
        self.get_op_revenue_data()

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
        
        # apply changes button 
        self.wait_until_clickable(APPLY_CHANGES_BUTTON)
        self.driver.find_element(By.XPATH, APPLY_CHANGES_BUTTON).click()
        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXCEL_BUTTON), 'Excel'))
        self.driver.find_element(By.XPATH, EXCEL_BUTTON).click()
        

        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXPORT_BUTTON), 'Export'))
        self.driver.find_element(By.XPATH, EXPORT_BUTTON).click()
        
                
        self.wait_until_clickable(POPUP_DOWNLOAD_BUTTON)

        
        while True:
            download_button= self.driver.find_element(By.XPATH, POPUP_DOWNLOAD_BUTTON)
            if download_button.value_of_css_property("background-color") == "rgba(0, 20, 137, 1)":
                download_button.send_keys(Keys.RETURN)                
                break
            else: 
                time.sleep(2)
                
    
    def strip_new_lines(self, df, colunm_name='Licensee'):
        df[colunm_name] = df[colunm_name].apply(lambda x: x.strip('\n'))
        return df 
    
    def prepare_data(self, df, df_orbis):
        related_data = df[['Licensee','Licensee CIK 1_cleaned','Licensor','Agreement Date']]
        # drop rows where Licensee CIK 1_cleaned is null
        related_data = related_data[related_data['Licensee CIK 1_cleaned'].notna()]
        
        
        related_data['Agreement Date'] = pd.to_datetime(related_data['Agreement Date'])
        related_data['Financial Period'] = related_data['Agreement Date'].dt.year - 1
        # merging 
        # related_data['Licensee'] = related_data['Licensee'].astype(str).str.upper()
        # df_orbis['Company name Latin alphabet'] = df_orbis['Company name Latin alphabet'].astype(str).str.upper()
        # df_orbis['Other company ID number'] = df_orbis['Other company ID number'].astype(str).str.upper()
        related_data = related_data.rename(columns={'Licensee CIK 1_cleaned': 'CIK'})
        df_orbis = df_orbis.rename(columns={'Other company ID number': 'CIK'})
        df_merged = df_orbis.merge(related_data, on='CIK')
        pattern = r'\D'
        # Use the `filter()` function to select the columns that contain "Operating revenue" in the column names
        # df_merged_filtered = df_merged.filter(like='Operating revenue')
        # Use the `rename()` function and the `re.sub()` function to apply the regex pattern to the column names
        df_merged_filtered = df_merged.rename(columns=lambda x:  re.sub(pattern, '', x) if x.startswith('Operating revenue') else x)

        df_merged_filtered.insert(len(df_merged_filtered.columns), "Operating revenue (Turnover)m USD", np.nan)
        for index, r in df_merged_filtered.iterrows():
            year = r['Financial Period']
            try: 
                revenue = r[str(year)]
                df_merged_filtered.at[index, 'Operating revenue (Turnover)m USD'] = revenue
            except KeyError as ke: 
                print(ke)
        
        df_merged_filtered=df_merged_filtered.drop(['Unnamed: 0'],axis=1)
        
        non_digit_cols = []
        for i in df_merged_filtered.columns:
            if i.isdigit():
                continue
            else:
                non_digit_cols.append(i)
                
        
        return df_merged_filtered[non_digit_cols]
    
    def to_xlsx(self, df, file_name):
        df.to_excel(file_name)
        
    

        


if __name__ == "__main__":
    config_path = "./config/config.yaml"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # add command line parser    
    with Orbis(config_path) as orbis:
        # orbis.init_driver()
        # orbis.login()
        # orbis.batch_search()
        # orbis.logout()
        
        # adds financial data operation revenue of the licensee agreement date
        
        
        
        # data processing and augmentation
        # after orbis search step following needs to run 
        df_licensee_data = orbis.read_xlxs_file(orbis.license_data)
        df_orbis_data = orbis.read_xlxs_file(orbis.orbis_data, sheet_name='Results')
        df_licensee = orbis.strip_new_lines(df_licensee_data)
        df_result = orbis.prepare_data(df_licensee, df_orbis_data)
        orbis.to_xlsx(df_result, orbis.data_path + f"processed_data_{timestamp}.xlsx")
        
        

