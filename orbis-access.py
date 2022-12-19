# Description: This script applies batch search on the Orbis website
# Python Version: 3.9.5
# Python Libraries: selenium, pandas, yaml

import time 
from os import path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import yaml 


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
IDENTIFICATION_NUMBER_VIEW = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]'
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
        self.chrome_options = webdriver.ChromeOptions()
        # for options in self.driver_options:
        #     self.chrome_options.add_argument(options)
        self.chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(executable_path=self.executable_path, chrome_options=self.chrome_options)
    
    def __exit__(self, exc_type, exc_value, traceback):
        return self.driver.close()
        
    def __enter__(self):
        return self
    
    
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
        
        
    def read_xlxs_file(self, file_path):
        df = pd.read_excel(file_path)
        return df
    
    def batch_search(self):
        
        self.driver.get(self.orbis_batch_search_url)
        # time.sleep(2)
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, DRAG_DROP_BUTTON)))
        self.driver.find_element(By.XPATH, DRAG_DROP_BUTTON).click()
        # time.sleep(2)
        
        file_input=self.driver.find_element(By.XPATH, SELECT_FILE_BUTTON)
        file_input.send_keys(self.data_path)
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, UPLOAD_BUTTON )))

        self.driver.find_element(By.XPATH, UPLOAD_BUTTON).click()
    
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, FIELD_SEPERATOR)))
        self.driver.find_element(By.XPATH, FIELD_SEPERATOR  ).clear()
        time.sleep(2)

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).send_keys(";")

        self.driver.find_element(By.XPATH, FIELD_SEPERATOR).send_keys(Keys.RETURN)
        
        print("field seperator is cleared")
        time.sleep(2)
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, APPLY_BUTTON)))

        self.driver.find_element(By.XPATH, APPLY_BUTTON).click()
        
        time.sleep(5)
        WebDriverWait(self.driver, 30*60).until(EC.invisibility_of_element_located((By.XPATH, SEARCH_PROGRESS_BAR)))

        
        warning_message = self.driver.find_element(By.XPATH, SEARCH_PROGRESS_BAR)
        while warning_message.text == "Search is not finished":
                time.sleep(5)
                warning_message = self.driver.find_element(By.XPATH, SEARCH_PROGRESS_BAR)
                print(warning_message.text)
        
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, VIEW_RESULTS_BUTTON)))
        
        view_result_sub_url = self.driver.find_element(By.XPATH, VIEW_RESULTS_BUTTON)
        view_result_sub_url.send_keys(Keys.RETURN)
       
        # add/remove columns //*[@id="main-content"]/div/div[2]/div[1]/a
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, ADD_REMOVE_COLUMNS_VIEW)))
        self.driver.find_element(By.XPATH, ADD_REMOVE_COLUMNS_VIEW).click()
        
        
        # WebDriverWait(self.driver, 30*60).until(EC.presence_of_element_located((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div')))
        # element = self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div')
        # self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
        
        # identification number column 
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, IDENTIFICATION_NUMBER_VIEW)))
        self.driver.find_element(By.XPATH, IDENTIFICATION_NUMBER_VIEW).click()
        
        # add BVD ID number column
       
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, BVD_ID_NUMBER_ADD)))
        self.driver.find_element(By.XPATH, BVD_ID_NUMBER_ADD).click()
        
        # add ORBIS ID number column
       
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, ORBIS_ID_NUMBER_ADD)))
        self.driver.find_element(By.XPATH, ORBIS_ID_NUMBER_ADD).click()

        time.sleep(2)
        # scroll down within in panel 
        self.scroll_to_bottom()
        time.sleep(1)
        
        # Ownership Data > //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div
       
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, OWNERSHIP_COLUMN)))
        self.driver.find_element(By.XPATH, OWNERSHIP_COLUMN).click()
        
        
        # scroll down within in panel 
        self.scroll_to_bottom()
        time.sleep(1)
        
        # Shareholders //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH,SHAREHOLDERS_COLUMN )))
        self.driver.find_element(By.XPATH, SHAREHOLDERS_COLUMN).click()
       
        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Information 
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, GUO_COLUMN)))
        self.driver.find_element(By.XPATH, GUO_COLUMN).click()
       
        self.scroll_to_bottom()
        time.sleep(1)
        # Global Ultimate Owner Name //*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, GUO_NAME_INFO)))
        self.driver.find_element(By.XPATH, GUO_NAME_INFO).click()
        
        self.scroll_to_bottom()
        time.sleep(1)
        # Immediate parent company name //*[@id="GUO*GUO.PARENT_NAME:UNIVERSAL"]/div[2]/span
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, IMMEDIATE_PARENT_COMPANY_NAME)))
        self.driver.find_element(By.XPATH, IMMEDIATE_PARENT_COMPANY_NAME).click()
        
        
        # ISH > //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[16]/div
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, ISH_NAME)))
        self.driver.find_element(By.XPATH, ISH_NAME).click()
        
        # apply changes button 
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, APPLY_CHANGES_BUTTON)))
        self.driver.find_element(By.XPATH, APPLY_CHANGES_BUTTON).click()
        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXCEL_BUTTON), 'Excel'))
        self.driver.find_element(By.XPATH, EXCEL_BUTTON).click()
        

        
        # '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a' >> export button
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, EXPORT_BUTTON), 'Export'))
        self.driver.find_element(By.XPATH, EXPORT_BUTTON).click()
        
        
        # # //*[@id="exportDialogForm"]/div[2]/a[2] >> download button from popup window
        # self.driver.find_element(By.XPATH, '//*[@id="exportDialogForm"]/div[2]/a[2]').click()
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, POPUP_DOWNLOAD_BUTTON)))

        # /html/body/section[2]/div[6]/div[3]/a >> download button from popup window
        
        while True:
            download_button= self.driver.find_element(By.XPATH, POPUP_DOWNLOAD_BUTTON)
            if download_button.value_of_css_property("background-color") == "rgba(0, 20, 137, 1)":
                download_button.send_keys(Keys.RETURN)                
                break
            else: 
                time.sleep(2)
       
        # time.sleep(180*10)


if __name__ == "__main__":
    config_path = "./config/config.yaml"
    with Orbis(config_path) as orbis:
        orbis.login()
        orbis.batch_search()
        orbis.logout()

