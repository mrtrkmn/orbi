import csv
import hashlib
import os
import re
import sys
import json
import time
from datetime import (
    datetime,
)
from threading import (
    Thread,
)

import pandas as pd
import requests
import yaml
from bs4 import (
    BeautifulSoup,
)

# Description: This script crawls the IPO website for the patent-related data and SEC.gov for the company-related data
#             and stores them in a csv file.
# Python Version: 3.9.5
# Python Libraries: requests, bs4, threading, time, pandas, csv, re


# this path append is needed for pdoc to generate the documentation
root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)


# class to crawl the IPO website for the patent-related data
class Crawler:

    """
    Crawler class to crawl the IPO website for the patent-related data
    CIK numbers are fetched from SEC.gov.
    """

    def __init__(
        self,
    ):
        """
        Initialize the crawler

        :param ipo_data: dictionary of the IPO data
        :param ipo_url: url of the IPO website
        :param sec_cik_numbers: dictionary of the CIK numbers {"company_name": "cik_number"}
        """
        self.ipo_data = {}
        self.ipo_url = "https://www.ipo.gov.uk/p-ipsum/Case/PublicationNumber/"
        # set of companies and cik_numbers such as {"company_name":
        # "cik_number"}
        self.sec_cik_numbers = {}

    # crawl the IPO website for the patent-related data

    def find_publication(
        self,
        publication_number,
    ):
        """
        Find the publication number on the IPO website and add the data to the dictionary

        :param publication_number: publication number of the IPO
        """

        # get the html content of the page
        url = f"{self.ipo_url}{publication_number}"

        response = requests.get(url)
        if response.status_code == 200:
            html = response.text
        else:
            raise Exception(f"Failed to get HTML content: {response.text}")

        # parse the html content
        soup = BeautifulSoup(
            html,
            "html.parser",
        )
        # find the table with the publication details
        try:
            table = soup.find(
                "table",
                {"class": "BibliographyTable"},
            )
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
            header = row.find_all(
                "td",
                {"class": "CaseDataItemHeader"},
            )[0].text
            value = row.find_all(
                "td",
                {"class": "CaseDataItemValue"},
            )[0].text
            self.ipo_data[header] = value

        # publication_no = all_data.find(class_="CaseHeader").text

    def __enter__(
        self,
    ):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        # empty the dictionaries
        self.ipo_data.clear()
        self.sec_cik_numbers.clear()

    def check_cik_number_format(
        self,
        cik_number,
    ):
        """
        Check if the CIK number is 10 digits
        :param cik_number: CIK number of the company
        """
        # check if the cik number is 10 digits
        cik_number = str(cik_number)
        if re.match(
            r"^\d{10}$",
            cik_number,
        ):
            return True
        return False

    def get_publication(
        self,
    ):
        # return the publication details
        """
        Return all the publication details from the dictionary saved in the class
        """

        return self.ipo_data

    def get_data_from_sec_gov(
        self,
        cik_number,
    ):
        """
        Retrieve the json data from the SEC.gov website

        :param cik_number: CIK number of the company
        """
        # get the json data from the SEC.gov website
        # https://data.sec.gov/submissions/CIK##########.json
        url = f"https://data.sec.gov/submissions/CIK{cik_number}.json"
        print(f"requesting data from {url}")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        }
        response = requests.get(
            url,
            headers=headers,
        )
        if response.status_code == 200:
            json_data = response.json()
        else:
            raise Exception(f"Failed to get JSON data: {response.text}")

        return json_data

    def get_cik_number_fy_columns(self, excel_file, is_licensee=False):
        # get Licensee CIK 1_cleaned and Agreement Date
        # substract one year from the Agreement Date

        df = pd.read_excel(excel_file)
        if is_licensee:
            df = df[["Licensee CIK 1_cleaned", "Agreement Date"]]
            df["Agreement Date"] = pd.to_datetime(df["Agreement Date"])
            df["Agreement Date"] = df["Agreement Date"].dt.year - 1
            df = df.rename(
                columns={
                    "Licensee CIK 1_cleaned": "CIK Number",
                    "Agreement Date": "FY",
                }
            )
            # drop the rows with invalid CIK Number
            df = df[df["CIK Number"].apply(self.check_cik_number_format)]
        else:
            df = df[["Licensee CIK 1_cleaned", "Agreement Date"]]
            df["Agreement Date"] = pd.to_datetime(df["Agreement Date"])
            df["Agreement Date"] = df["Agreement Date"].dt.year - 1
            df = df.rename(
                columns={
                    "Licensee CIK 1_cleaned": "CIK Number",
                    "Agreement Date": "FY",
                }
            )
        # convert the CIK Number to int

        # return only the CIK Number and FY columns
        df = df[["CIK Number", "FY"]].dropna(subset=["CIK Number"])
        # df["CIK Number"] = df["CIK Number"].astype(int)
        return df

    def get_company_facts_data(self, df):
        """
        Retrieve the company facts data from the json data

        :param json_data: json data from the SEC.gov website
        """
        json_data = {}
        # df contains the CIK Number and FY columns
        # get the json data from the SEC.gov website
        # "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number}.json"
        for index, row in df.iterrows():
            is_us_gaap = False
            cik_number = row["CIK Number"]
            fy = row["FY"]
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number}.json"
            print(f"requesting company facts data from {url}")
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            }
            try:
                response = requests.get(
                    url,
                    headers=headers,
                )
            except Exception as e:
                print(e)
                continue

            if response.status_code == 200:
                company_facts_data = response.json()
            else:
                print(f"Failed to get JSON data: {response.text}")
                continue

            # raw_data = company_facts_data["JSON"]
            company_name = company_facts_data["entityName"]
            cik_number = company_facts_data["cik"]

            # check if ["facts"]["us-gaap"] exists in the json data
            if "us-gaap" in company_facts_data["facts"]:
                print("us-gaap  in company_facts_data['facts']")
                if not "GrossProfit" in company_facts_data["facts"]["us-gaap"]:
                    print(f"GrossProfit not in company_facts_data['facts']['us-gaap'] for {cik_number}")
                    continue

                gross_profit_usd = company_facts_data["facts"]["us-gaap"]["GrossProfit"]["units"]["USD"]
                is_us_gaap = True

            if not is_us_gaap and "ifrs-full" in company_facts_data["facts"]:
                print("ifrs-full not in company_facts_data['facts']")
                if not "GrossProfit" in company_facts_data["facts"]["ifrs-full"]:
                    print(f"GrossProfit not in company_facts_data['facts']['ifrs-full'] for {cik_number}")
                    continue

                gross_profit_usd = company_facts_data["facts"]["ifrs-full"]["GrossProfit"]["units"]["DKK"]
                is_us_gaap = False

            for value in gross_profit_usd:
                if value["fy"] == fy:
                    form_type = value["form"]
                    start_date = value["start"]
                    end_date = value["end"]
                    financial_year = value["fy"]
                    gross_profit = value["value"]
                    print(
                        f"company_name: {company_name} form_type: {form_type}, start_date: {start_date}, end_date: {end_date}, financial_year: {financial_year}, gross_profit: {gross_profit}"
                    )

    def parse_company_financial_info(
        self,
        company_facts_data,
    ):
        """
        Parse the company financial information from the json data

        :param json_data: json data from the SEC.gov website
        """
        raw_data = company_facts_data["JSON"]
        company_name = company_facts_data["entityName"]
        cik_number = company_facts_data["cik"]

        gross_profit_usd = company_facts_data["facts"]["us-gaap"]["GrossProfit"]["units"]["USD"]

        for key, value in gross_profit_usd.items():
            form_type = gross_profit_usd[key]["form"]
            start_date = gross_profit_usd[key]["start"]
            end_date = gross_profit_usd[key]["end"]
            financial_year = gross_profit_usd[key]["fy"]

    def get_data_from_sec_gov_in_parallel(
        self,
        url,
        company_name,
        results,
    ):
        # get the json data from the SEC.gov website
        # https://data.sec.gov/submissions/CIK##########.json

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        }
        json_data = {}

        if url == "":
            json_data["name"] = company_name
            json_data["cik_number"] = ""
            print(f"No CIK number to look up for : {company_name}")
        else:
            cik_number = url.split(".json")[0].split("CIK")[1]
            print(f"requesting data from {url}")
            response = requests.get(
                url,
                headers=headers,
            )
            if response.status_code == 200:
                json_data = response.json()
            else:
                raise Exception(f"Failed to get JSON data: {response.text}")
            json_data["cik_number"] = cik_number

        results.append(json_data)

    def lookup_cik(
        self,
        company_name,
    ):
        """
        Lookup the CIK number of the company on the SEC.gov website
        :param company_name: name of the company
        """

        # get the html content of the page
        # https://www.sec.gov/edgar/searchedgar/cik
        url = f"https://www.sec.gov/cgi-bin/cik_lookup"
        params = {
            "company": company_name,
            "MIME Type": "application/x-www-form-urlencode",
        }
        response = requests.post(
            url,
            data=params,
        )

        if response.status_code == 200:
            html = response.text
        else:
            raise Exception(f"Failed to get HTML content: {response.text}")
        soup = BeautifulSoup(
            html,
            "html.parser",
        )

        try:
            # find the table with the publication details
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
                cik_number = row.find_all("a")[0].text
                self.sec_cik_numbers[company_name] = cik_number
            except IndexError as e:
                print(f"Failed to find the cik number: {e}")
                continue

    # get all cik numbers
    def get_existing_cik_numbers(
        self,
    ):
        """
        Get all the CIK numbers from the dictionary saved in the class
        """
        return self.sec_cik_numbers

    # read xlxs file with pandas

    def read_xlxs_file(
        self,
        file_path,
    ):
        """
        Generate a pandas dataframe from the excel file
        :param file_path: path to the excel file
        """
        df = pd.read_excel(
            file_path,
            engine="openpyxl",
        )
        return df

    # check if the state code belongs to the US
    def is_usa(
        self,
        state_code,
    ):
        """
        Check if the state code belongs to the US
        :param state_code: state code of the company
        """
        states = [
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
        ]
        if state_code in states:
            return True
        return False

    def read_config(
        self,
        path,
    ):
        """
        Read the config file
        :param path: path to the config file
        """
        # read from yaml config file
        with open(
            path,
            "r",
        ) as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                raise exc
        return config


def generate_unique_id(
    company_name,
    n=25,
):
    """
    Generate a unique id for the company; hash company name
    :param company_name: name of the company
    """
    sha256 = hashlib.sha256()
    sha256.update(str(company_name).encode())
    return sha256.hexdigest()[:n]


def save_raw_data(
    results,
    output_file,
):
    """
    Save the raw data from the SEC.gov website
    :param results: list of dictionaries with the data from the SEC.gov website
    :param output_file: path to the output file
    """
    with open(
        output_file,
        "w",
    ) as f:
        for result in results:
            try:
                street1 = result["addresses"]["business"]["street1"]
            except KeyError as e:
                street1 = "No street1 found"

            try:
                street2 = result["addresses"]["business"]["street2"]
            except KeyError as e:
                street2 = "No street2 found"

            try:
                city = result["addresses"]["business"]["city"]
            except KeyError as e:
                city = "No city found"

            try:
                state = result["addresses"]["business"]["state"]
            except KeyError as e:
                state = "No state found"

            try:
                zipCode = result["addresses"]["business"]["zipCode"]
            except KeyError as e:
                zipCode = "No zipCode found"

            try:
                stateOrCountry = result["addresses"]["business"]["stateOrCountry"]
            except KeyError as e:
                stateOrCountry = "No stateOrCountry found"

            try:
                stateOrCountryDescription = result["addresses"]["business"]["stateOrCountryDescription"]
            except KeyError as e:
                stateOrCountryDescription = "No stateOrCountryDescription found"

            raw_data = (
                str(street1)
                + ", "
                + str(street2)
                + ", "
                + str(city)
                + ", "
                + str(state)
                + ", "
                + str(zipCode)
                + ", "
                + str(stateOrCountry)
                + ", "
                + str(stateOrCountryDescription)
            )

            try:
                # f.write(json.dumps(result["addresses"]["business"]))
                f.write(raw_data)
                f.write("\n")
            except KeyError as e:
                f.write("No address found\n")
                continue


def get_company_facts(source_file, output_file, is_licensee=False):
    """
    Get the company facts from the SEC.gov website
    :param source_file: path to the input file
    :param output_file: path to the output file
    :param is_licensee: boolean value to indicate if the source file is for licensee or licensor
    """
    with Crawler() as crawler:
        results = []
        # get the data from the SEC.gov website
        results = crawler.get_company_facts(source_file, is_licensee)
        # save the raw data
        save_raw_data(results, output_file)


# prepare_data generates the file which needs to be used in orbi.py first step
#  source_file: provided by the user
# output_file: csv file with the data from the SEC.gov website (columns:
# company name, city, country, identifier)


def create_input_file_for_orbis_batch_search(
    source_file,
    output_file,
    is_licensee=False,
):
    """
    Create the input file for the Orbis batch search; CSV file with the data from the SEC.gov website

    :param source_file: provided by the user
    :param output_file: csv file with the data from the SEC.gov website (columns: company name, city, country, identifier)
    :param is_licensee: boolean value to indicate if the source file is for licensee or licensor
    """

    with Crawler() as crawler:
        results = []
        # get

        df = crawler.read_xlxs_file(source_file)
        if is_licensee:
            cols = [
                "Licensee 1_cleaned",
                "Licensee CIK 1_cleaned",
            ]
            df = df.drop_duplicates("Licensee 1_cleaned")
        else:
            cols = [
                "Licensor 1_cleaned",
                "Licensor CIK 1_cleaned",
            ]
            df = df.drop_duplicates("Licensor 1_cleaned")

        # get two columns
        df = df[cols]
        df = df.replace(
            r"\n",
            " ",
            regex=True,
        )
        # create file on data folder
        columns = [
            "company name",
            "city",
            "country",
            "identifier",
        ]
        # Set the maximum number of requests per second

        max_requests_per_second = 10  # defined by SEC.gov
        company_info = {}

        for (
            index,
            row,
        ) in df.iterrows():
            try:
                company_name = row[0]

                if company_name == "":
                    continue
                cik_number = row[1]
            except IndexError as e:
                raise Exception(f"Index error: {e}")
            if crawler.check_cik_number_format(cik_number):
                company_info[company_name] = f"https://data.sec.gov/submissions/CIK{cik_number}.json"
            else:
                company_info[company_name] = ""

        threads = [
            Thread(
                target=crawler.get_data_from_sec_gov_in_parallel,
                args=(
                    url,
                    company_name,
                    results,
                ),
            )
            for company_name, url in company_info.items()
        ]

        for thread in threads:
            thread.start()
            time.sleep(1 / max_requests_per_second)

        for thread in threads:
            thread.join()

        save_raw_data(
            results,
            "raw_data_from_sec_gov.json",
        )

        with open(
            output_file,
            "w",
        ) as f:
            w = csv.writer(
                f,
                delimiter=";",
            )
            w.writerow(columns)

            for result in results:
                try:
                    name = result["name"]
                except KeyError as e:
                    print(f"Failed to find the key: {e} for {result['name']}")
                    continue

                try:
                    city = result["addresses"]["business"]["city"]
                except KeyError as e:
                    print(f"Failed to find the key: {e} for {result['name']}")
                    city = ""

                try:
                    country = result["addresses"]["business"]["stateOrCountryDescription"]
                except KeyError as e:
                    print(f"Failed to find the key: {e} for {result['name']}")
                    country = ""

                try:
                    identifier = result["cik_number"]
                except KeyError as e:
                    print(f"Failed to find the key: {e} for {result['name']}")
                    identifier = ""

                if crawler.is_usa(country):
                    country = "United States of America"
                w.writerow(
                    [
                        name,
                        city,
                        country,
                        identifier,
                    ]
                )


# In case of running only crawler part
# ----------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    print(os.path.dirname(os.path.abspath("data")))
    data_path = os.path.join("sample_data.xlsx")
    timestamp = datetime.now().strftime("%d_%m_%Y")
    # crawler.find_publication("GB2419368")
    # print(crawler.get_publication())
    # company_name = "Apple Inc."
    # crawler.lookup_cik(company_name)
    # cik_number = crawler.get_existing_cik_numbers()[company_name]
    # print(crawler.get_data_from_sec_gov(cik_number))
    # print(crawler.get_data_from_sec_gov_in_parallel(cik_number))
    # print(os.path.dirname(os.path.abspath('data')))
    # create_input_file_for_orbis_batch_search(
    #     os.path.join(
    #         os.path.abspath("data"),
    #         "sample_data.xlsx",
    #     ),
    #     f"orbis_data_{timestamp}.csv",
    #     is_licensee=True,
    # )
    with Crawler() as crawler:
        # crawler.get_company_facts(
        #     os.path.join(
        #         os.path.abspath("data"),
        #         "sample_data.xlsx",
        #     ),
        #     f"company_facts_{timestamp}.json",
        #     is_licensee=True,
        # )

        fy_cik_df = crawler.get_cik_number_fy_columns(
            os.path.join(os.path.abspath("data"), "sample_data.xlsx"), is_licensee=True
        )
        # get company facts
        crawler.get_company_facts_data(fy_cik_df)

    # print(fy_cik_df)
# -------------------------------------------------------------------------------------------------------------------------------
