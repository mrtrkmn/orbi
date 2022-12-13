import requests
from bs4 import BeautifulSoup
from threading import Thread
import time 
import pandas as pd 
import csv 

class Crawler:

    def __init__(self):
        self.ipo_data = {}
        self.ipo_url = "https://www.ipo.gov.uk/p-ipsum/Case/PublicationNumber/"
        self.sec_cik_numbers = {} # set of companies and cik_numbers such as {"company_name": "cik_number"}

    # crawl the IPO website for the patent-related data
    def find_publication(self, publication_number):
        url = f"{self.ipo_url}{publication_number}"
        
        response = requests.get(url)
        if response.status_code == 200:
            html = response.text
        else:
            raise Exception(f"Failed to get HTML content: {response.text}")

        soup = BeautifulSoup(html, "html.parser")
        # find the table with the publication details

        try:
            table = soup.find("table", {"class": "BibliographyTable"})
        except AttributeError as e:
            raise Exception(f"Failed to find the table: {e}")

        # find the rows in the table
        try: 
            rows = table.find_all("tr")
        except AttributeError as e:
            raise Exception(f"Failed to find the rows: {e}")
            

        # find the first row
        for row in rows:
            # find the first column
            header = row.find_all("td", {"class": "CaseDataItemHeader"})[0].text
            value  = row.find_all("td", {"class": "CaseDataItemValue"})[0].text
            self.ipo_data[header] = value

        # publication_no = all_data.find(class_="CaseHeader").text
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.ipo_data = {}
        self.sec_cik_numbers = {}



    def get_publication(self):
        return self.ipo_data


    # https://data.sec.gov/submissions/CIK##########.json
    def get_data_from_sec_gov(self, cik_number):
        url = f"https://data.sec.gov/submissions/CIK{cik_number}.json"
        print (f"requesting data from {url}")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"

        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
        else:
            raise Exception(f"Failed to get JSON data: {response.text}")

        return json_data

    def get_data_from_sec_gov_in_parallel(self, url, results):
        print (f"requesting data from {url}")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"

        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
        else:
            raise Exception(f"Failed to get JSON data: {response.text}")
        results.append(json_data)
        # return json_data
    
    # https://www.sec.gov/edgar/searchedgar/cik
    def lookup_cik(self, company_name):
        url = f"https://www.sec.gov/cgi-bin/cik_lookup"
        params = {"company": company_name,"MIME Type": "application/x-www-form-urlencode"}
        response = requests.post(url, data=params)
        if response.status_code == 200:
            html = response.text
        else:
            raise Exception(f"Failed to get HTML content: {response.text}")
        soup = BeautifulSoup(html, "html.parser")
        try:
            table = soup.find("table")
        except AttributeError as e:
            raise Exception(f"Failed to find the table: {e}")

        # find the rows in the table
        try:
            rows = table.find_all("tr")
        except AttributeError as e:
            raise Exception(f"Failed to find the rows: {e}")
        
        # find the first row
        for row in rows:
            # find the first column
            try: 
                cik_number  = row.find_all("a")[0].text
                self.sec_cik_numbers[company_name] = cik_number
            except IndexError as e:
                print(f"Failed to find the cik number: {e}")
                continue

    def get_existing_cik_numbers(self):
        print("Make sure you run the lookup_cik method first for the company name")
        return self.sec_cik_numbers


    # read xlxs file with pandas 
    def read_xlxs_file(self, file_path):
        df = pd.read_excel(file_path)
        return df

    def is_usa(self,state_code):
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        if state_code in states:
            return True
        return False

    
if __name__ == "__main__":
    crawler = Crawler()
    results = []
    # crawler.find_publication("GB2419368")
    # print(crawler.get_publication())
    # company_name = "Apple Inc."
    # crawler.lookup_cik(company_name)
    # cik_number = crawler.get_existing_cik_numbers()[company_name]
    # print(crawler.get_data_from_sec_gov(cik_number))
 
    df = crawler.read_xlxs_file("data/sample_data.xlsx")
    print(df.columns)
    # get two columns
    print(df[["Licensee 1_cleaned", "Licensee CIK 1_cleaned"]])

    # create file on data folder
    columns = ["name", "city", "country"] 
    # Set the maximum number of requests per second

    max_requests_per_second = 10     # defined by SEC.gov

    cik_numbers = list(df["Licensee CIK 1_cleaned"])
    urls = []
    for cik_number in cik_numbers:
        if pd.isna(cik_number):
            continue
        urls.append(f"https://data.sec.gov/submissions/CIK{cik_number}.json")

    threads = [Thread(target=crawler.get_data_from_sec_gov_in_parallel, args=(url, results)) for url in urls]

    for thread in threads:
        thread.start()
        time.sleep(1 / max_requests_per_second)
        
    for thread in threads:
        thread.join()


    with open("data/data.csv", "w") as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(columns)

        for result in results:
            try:
                name = result['name']
                city = result['addresses']['business']['city']
                country = result['addresses']['business']['stateOrCountryDescription']
                if crawler.is_usa(country):
                    country = "United States of America"
                # cik_number = result["cikNumber"]
                w.writerow([name, city, country])
            except KeyError as e:
                print(f"Failed to find the key: {e}")
                continue
       