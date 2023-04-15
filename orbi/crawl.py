import csv
from email import header
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
import concurrent.futures


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


NUMBER_OF_EMPLOYEES = "EntityNumberOfEmployees"
OPERATING_INCOME_LOSS = "OperatingIncomeLoss"
NET_INCOME_LOSS = "NetIncomeLoss"
NET_CASH_USED_IN_OP_ACTIVITIES = "NetCashProvidedByUsedInOperatingActivities"
NET_CASH_USED_IN_INVESTING_ACTIVITIES = "NetCashProvidedByUsedInInvestingActivities"
NET_CASH_USED_IN_FINANCING_ACTIVITIES = "NetCashProvidedByUsedInFinancingActivities"
CASH_EQUIVALENTS = (
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"
)
INCOME_LOSS_BEFORE_CONT_OPS = (
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
)
GROSS_PROFIT = "GrossProfit"
ASSETS = "Assets"


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
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        }
        self.kpi_variables = [
            NUMBER_OF_EMPLOYEES,
            OPERATING_INCOME_LOSS,
            NET_INCOME_LOSS,
            NET_CASH_USED_IN_OP_ACTIVITIES,
            NET_CASH_USED_IN_INVESTING_ACTIVITIES,
            NET_CASH_USED_IN_FINANCING_ACTIVITIES,
            CASH_EQUIVALENTS,
            INCOME_LOSS_BEFORE_CONT_OPS,
            GROSS_PROFIT,
            ASSETS,
        ]

    def recursive_lookup(self, data, key):
        if isinstance(data, dict):
            for k, v in data.items():
                if k == key:
                    yield v
                yield from self.recursive_lookup(v, key)
        elif isinstance(data, list):
            for item in data:
                yield from self.recursive_lookup(item, key)

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
            df = df[["Licensee 1_cleaned", "Licensee CIK 1_cleaned", "Agreement Date"]]
            df["Agreement Date"] = pd.to_datetime(df["Agreement Date"])
            # df["Agreement Date"] = df["Agreement Date"].dt.year - 1
            df = df.rename(
                columns={
                    "Licensee CIK 1_cleaned": "CIK Number",
                    "Licensee 1_cleaned": "Company Name",
                }
            )
            # drop the rows with invalid CIK Number
            df = df[df["CIK Number"].apply(self.check_cik_number_format)]
        else:
            df = df[["Licensor 1_cleaned", "Licensor CIK 1_cleaned", "Agreement Date"]]
            df["Agreement Date"] = pd.to_datetime(df["Agreement Date"])
            # df["Agreement Date"] = df["Agreement Date"].dt.year - 1
            df = df.rename(
                columns={
                    "Licensee CIK 1_cleaned": "CIK Number",
                    "Licensor 1_cleaned": "Company Name",
                }
            )
        # convert the CIK Number to int

        df = df[["CIK Number", "Agreement Date", "Company Name"]].dropna(subset=["CIK Number"])
        # drop duplicates
        df = df.drop_duplicates(subset=["CIK Number"])
        # df["CIK Number"] = df["CIK Number"].astype(int)
        print(f"Columns in the dataframe: {df.columns}")
        return df

    def write_to_file(self, file_name, info):
        # write the info to the file
        with open(file_name, "a") as f:
            f.write(info + "\n")

    def get_company_raw_data(self, company_name, cik_number):
        """
        Request to the endpoint to get the company raw data
        If the request is successful, return the json data
        If not, return an empty dictionary
        """

        company_facts_data = {}
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number}.json"

        try:
            response = requests.get(url, headers=self.headers)
        except Exception as e:
            print(f"Error on fetching: {e}")
            self.write_to_file("not_found_companies.txt", f"{company_name} , {cik_number}")
            return company_facts_data

        if response.status_code == 200:
            company_facts_data = response.json()
        else:
            print(f"Failed to get JSON data {response.text}; URL: {url}")
            self.write_to_file("not_found_companies.txt", f"{company_name} , {cik_number}")
            return company_facts_data

        return company_facts_data
    


    def parse_data_from_end_result(self, file_path: str):
        # read json file
        parsed_data = {}

        with open(file_path, "r") as f:
            json_data = json.load(f)
        # parse the data
        for k, v in json_data.items():
            for kpi_var in self.kpi_variables:
                try:
                    unit = list(json_data[k][kpi_var][0]["units"].keys())[0]
                    value = json_data[k][kpi_var][0]["units"][unit]
                    for i in value:
                        if i["form"] == "10-K":
                            agreement_date = json_data[k]["agreementDate"]

                            # convert agreement date to datetime object
                            agreement_date = datetime.strptime(agreement_date, "%Y-%m-%d")
                            end_date = datetime.strptime(i["end"], "%Y-%m-%d")

                            # subtract the two dates
                            diff = end_date - agreement_date

                            # convert the difference to positive number

                            if "diffInDays" not in json_data[k]:
                                json_data[k]["diffInDays"] = abs(diff.days)
                                val = i["val"]
                                company_name = json_data[k]["entityName"]
                                form_type = i["form"]

                            if abs(diff.days) < json_data[k]["diffInDays"]:
                                json_data[k]["diffInDays"] = diff.days
                                val = i["val"]
                                company_name = json_data[k]["entityName"]
                                form_type = i["form"]

                            if k not in parsed_data:
                                parsed_data[k] = {}

                            if "companyName" not in parsed_data[k]:
                                parsed_data[k]["companyName"] = {}

                            if company_name not in parsed_data[k]["companyName"]:
                                parsed_data[k]["companyName"] = company_name

                            if kpi_var not in parsed_data[k]:
                                parsed_data[k][kpi_var] = {}

                            parsed_data[k][kpi_var]["value"] = val

                            parsed_data[k][kpi_var]["unit"] = unit
                            parsed_data[k][kpi_var]["form"] = form_type
                            parsed_data[k]["diffInDays"] = diff.days
                            parsed_data[k]["agreementDate"] = json_data[k]["agreementDate"]
                            parsed_data[k]["endDate"] = i["end"]

                except KeyError as ke:
                    # print(f"Key error: {ke}")
                    # print(f"Key: {kpi_var}")
                    # print(f"File: {file_path}")
                    continue
        return parsed_data


    def json_to_csv(self, input_file: str, output_file: str):
        with open(input_file, "r") as f:
            json_data = json.load(f)

        not_financial = ["companyName", "agreementDate", "endDate", "diffInDays"]

        # take the keys from the json file for column headers
        for k, v in json_data.items():
            headers = list(json_data[k].keys())
            break

        financial_columns = set(headers) - set(not_financial)
        financial_columns = list(financial_columns)

        headers = not_financial + financial_columns

        # create a csv file
        with open(output_file, "w") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        data = ""
        # write the data to the csv file
        with open(output_file, "a") as f:
            for k, v in json_data.items():
                for i in not_financial:
                    data += str(json_data[k][i]) + ","
                for i in financial_columns:
                    if i in json_data[k]:
                        data += str(json_data[k][i]["value"]) + ","
                    else:
                        data += "NAN" + ","
                f.write(data + "\n")
                data = ""


    def get_company_facts_data(self, df):
        """
        Retrieve the company facts data from the json data

        :param json_data: json data from the SEC.gov website
        """
        json_data = {}
        # get the json data from the SEC.gov website
        # "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number}.json"
        for index, row in df.iterrows():
            cik_number = row["CIK Number"]
            company_name = row["Company Name"]
            agreement_date = row["Agreement Date"]
            # append result to json_data
            company_facts_data = self.get_company_raw_data(company_name, cik_number)
            # print(company_facts_data)
            if company_facts_data:
                # print(f"Processing {cik_number} {fy}")
                for kpi_var in self.kpi_variables:
                    # print(f"KPI Variable: {kpi_var}")
                    kpi_information = list(self.recursive_lookup(company_facts_data, kpi_var))
                    if kpi_information:
                        if cik_number not in json_data:
                            json_data[cik_number] = {}
                        if "agreementDate" not in json_data[cik_number]:
                            json_data[cik_number]["agreementDate"] = str(agreement_date.date())
                        if "entityName" not in json_data[cik_number]:
                            json_data[cik_number]["entityName"] = company_facts_data["entityName"]
                        if kpi_var not in json_data[cik_number]:
                            json_data[cik_number][kpi_var] = kpi_information

                    # json_data[kpi_var] = list(recursive_lookup(company_facts_data, kpi_var))
            else:
                continue
        # this is question mark whether it is good approach or not
        return json_data

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
            os.path.join(os.path.abspath("data"), "sample_data_big.xlsx"), is_licensee=True
        )
        # get company facts
        # print(crawler.get_company_facts_data(fy_cik_df))

        company_info = crawler.get_company_facts_data(fy_cik_df)
        with open(os.path.join(os.path.abspath("data"), f"company_facts_{timestamp}_big.json"), "w") as f:
            json.dump(company_info, f, indent=4)

        parsed_data = crawler.parse_data_from_end_result(os.path.join(os.path.abspath("data"),f"company_facts_{timestamp}_big.json"))
        with open(os.path.join(os.path.abspath("data"),f"parsed_company_facts_{timestamp}_big.json"), "w") as f:
            json.dump(parsed_data, f, indent=4)

        crawler.json_to_csv(input_file=os.path.join(os.path.abspath("data"),f"parsed_company_facts_{timestamp}_big.json"), output_file=os.path.join(os.path.abspath("data"),f"parsed_company_facts_{timestamp}_big.csv"))


    # print(fy_cik_df)
# -------------------------------------------------------------------------------------------------------------------------------
