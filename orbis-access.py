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


class Orbis:
    
    def __init__(self, config_path):
        config = self.read_config(config_path)
        print(config)
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
        login_button = self.driver.find_element(By.XPATH,"/html/body/div[2]/div/div[1]/div[1]/div[1]/form/fieldset/div[3]/input")
        # click the login button
        login_button.click()

    def logout(self):
        # logout from the web site
        time.sleep(3)
        self.driver.get(self.orbis_logout_url)
        
        
        
        
    def read_xlxs_file(self, file_path):
        df = pd.read_excel(file_path)
        return df
    
    def batch_search(self):
        
        self.driver.get(self.orbis_batch_search_url)
        # time.sleep(2)
        # WebDriverWait(self.driver, 20).until(EC.text_to_be_present_in_element((By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a"), 'View results'))
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a")))
        self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a").click()
        # time.sleep(2)
        file_input=self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[1]/div[1]/input[1]")
        file_input.send_keys(self.data_path)
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[2]/p/a[2]")))

        self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[2]/p/a[2]").click()
    
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input")))
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").clear()
        time.sleep(2)

        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").send_keys(";")

        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").send_keys(Keys.RETURN)
        
        print("field seperator is cleared")
        time.sleep(2)
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/section[2]/div[3]/div/form/div[3]/div[2]/input')))

        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[3]/div[2]/input").click()
        
        time.sleep(5)
        WebDriverWait(self.driver, 30*60).until(EC.invisibility_of_element_located((By.XPATH, '/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[1]')))

        
        warning_message = self.driver.find_element(By.XPATH, '/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[1]')
        while warning_message.text == "Search is not finished":
                time.sleep(5)
                warning_message = self.driver.find_element(By.XPATH, '/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[1]')
                print(warning_message.text)
        
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/section[2]/div[1]/div[2]/ul/li[1]')))
        
        
        
        
        view_result_sub_url = self.driver.find_element(By.XPATH, '/html/body/section[2]/div[1]/div[2]/ul/li[1]/a')
        view_result_sub_url.send_keys(Keys.RETURN)
       
        # add/remove columns //*[@id="main-content"]/div/div[2]/div[1]/a
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/a')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/a').click()
        
        
        # WebDriverWait(self.driver, 30*60).until(EC.presence_of_element_located((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div')))
        # element = self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/div')
        # self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
        
        # identification number column 
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]').click()
        
        # add BVD ID number column
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="IDENTIFIERS*IDENTIFIERS.BVD_ID_NUMBER:UNIVERSAL"]')))
        self.driver.find_element(By.XPATH, '//*[@id="IDENTIFIERS*IDENTIFIERS.BVD_ID_NUMBER:UNIVERSAL"]').click()
        
        # add ORBIS ID number column
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="IDENTIFIERS*IDENTIFIERS.ORBISID:UNIVERSAL"]')))
        self.driver.find_element(By.XPATH, '//*[@id="IDENTIFIERS*IDENTIFIERS.ORBISID:UNIVERSAL"]').click()

        time.sleep(2)
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        time.sleep(1)
        
        # Ownership Data > //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div').click()
        
        # Shareholders //*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div
        
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        time.sleep(1)
        
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div').click()
       
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        time.sleep(1)
        # Global Ultimate Owner Information 
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[4]/div')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[4]/div').click()
       
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        time.sleep(1)
        # Global Ultimate Owner Name //*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span')))
        self.driver.find_element(By.XPATH, '//*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span').click()
        
        self.driver.execute_script("document.getElementsByClassName('hierarchy-container')[0].scrollTo(0, document.getElementsByClassName('hierarchy-container')[0].scrollHeight)")
        time.sleep(1)
        # Immediate parent company name //*[@id="GUO*GUO.PARENT_NAME:UNIVERSAL"]/div[2]/span
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[3]/div')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[3]/div').click()
        
        
        # 
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ISH*ISH.ISH_NAME:UNIVERSAL"]')))
        self.driver.find_element(By.XPATH, '//*[@id="ISH*ISH.ISH_NAME:UNIVERSAL"]').click()
        
        # apply changes button 
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div/div[3]/form/div/input[2]')))
        self.driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[3]/form/div/input[2]').click()
        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a'), 'Excel'))
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a").click()
        

        
        # '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a' >> export button
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[5]/form/div[2]/a[2]'), 'Export'))
        self.driver.find_element(By.XPATH, '/html/body/section[2]/div[5]/form/div[2]/a[2]').click()
        
        
        # # //*[@id="exportDialogForm"]/div[2]/a[2] >> download button from popup window
        # self.driver.find_element(By.XPATH, '//*[@id="exportDialogForm"]/div[2]/a[2]').click()
        WebDriverWait(self.driver, 30*60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/section[2]/div[6]/div[3]/a')))

        # /html/body/section[2]/div[6]/div[3]/a >> download button from popup window
        
        while True:
            download_button= self.driver.find_element(By.XPATH, '/html/body/section[2]/div[6]/div[3]/a')
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

