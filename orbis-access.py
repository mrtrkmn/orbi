# access to web site using selenium 

import time 
import requests
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
        self.driver.get(self.orbis_logout_url)
        logout_button = self.driver.find_element(By.XPATH,"/html/body/section[2]/div[1]/div/div/div[1]/input")
        logout_button.click()
        
        
    def read_xlxs_file(self, file_path):
        df = pd.read_excel(file_path)
        return df
    
    def batch_search(self, file_path):
        
        self.driver.get(self.orbis_batch_search_url)
        time.sleep(2)
        # WebDriverWait(self.driver, 20).until(EC.text_to_be_present_in_element((By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a"), 'View results'))
        self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a").click()
        time.sleep(2)
        file_input=self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[1]/div[1]/input[1]")
        file_path = "/Users/mrturkmen/Desktop/idp-works/data/data.csv"
        file_input.send_keys(file_path)
        time.sleep(2)

        self.driver.find_element(By.XPATH, "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[2]/p/a[2]").click()
        # upload_field = form.find_element(By.CSS_SELECTOR,'input[name="id"]')
        time.sleep(4)
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").clear()
        time.sleep(2)
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").send_keys(";")
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input").send_keys(Keys.RETURN)
        print("field seperator is cleared")
        time.sleep(2)
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[3]/div/form/div[3]/div[2]/input").click()
        time.sleep(5)
        
                # '//*[@id="batch-search-toolbar"]/ul/li[1]/a' >> view results button
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[1]/div[2]/ul/li[1]/a'), 'View results'))
        
        
        view_result_sub_url = self.driver.find_element(By.XPATH, '/html/body/section[2]/div[1]/div[2]/ul/li[1]/a').get_attribute('href') 
        
        print(view_result_sub_url)
        # self.driver.find_element(By.XPATH, '/html/body/section[2]/div[1]/div[2]/ul/li[1]/a').click()
        self.driver.get(view_result_sub_url)
        
        
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a'), 'Excel'))
        self.driver.find_element(By.XPATH, "/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a").click()
        

        
        # '/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a' >> export button
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[5]/form/div[2]/a[2]'), 'Export'))
        self.driver.find_element(By.XPATH, '/html/body/section[2]/div[5]/form/div[2]/a[2]').click()
        
        
        # # //*[@id="exportDialogForm"]/div[2]/a[2] >> download button from popup window
        # self.driver.find_element(By.XPATH, '//*[@id="exportDialogForm"]/div[2]/a[2]').click()
        WebDriverWait(self.driver, 30*60).until(EC.text_to_be_present_in_element((By.XPATH, '/html/body/section[2]/div[6]/div[3]/a'), 'Download'))
        # /html/body/section[2]/div[6]/div[3]/a >> download button from popup window
        self.driver.find_element(By.XPATH, '/html/body/section[2]/div[6]/div[3]/a').click()
         
        time.sleep(10)
        # time.sleep(180*10)


if __name__ == "__main__":
    config_path = "./config/config.yaml"
    file_path = "./data/data.csv"
    with Orbis(config_path) as orbis:
        orbis.login()
        orbis.batch_search(file_path)
        orbis.logout()

