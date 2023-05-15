# author: mrtrkmn@github
# Make requests to receive data from SEC website
# Async and sync requests are possible, in some cases parallel execution might happen

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from threading import Thread

import aiohttp
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

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
COMPREHENSIVE_INCOME_NET_OF_TAX = "ComprehensiveIncomeNetOfTax"
CASH_EQUIVALENTS_PERIOD_INCREASE_DECREASE = "CashAndCashEquivalentsPeriodIncreaseDecrease"
# when grossprofit does not exists, use the following variables
# grossprofit = revenues - costofgoodssold
REVENUES = "Revenues"
COST_OF_GOODS_AND_SERVICES_SOLD = "CostOfGoodsAndServicesSold"


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
            f"{NUMBER_OF_EMPLOYEES} Reporting date",
            OPERATING_INCOME_LOSS,
            f"{OPERATING_INCOME_LOSS} Reporting date",
            NET_INCOME_LOSS,
            f"{NET_INCOME_LOSS} Reporting date",
            NET_CASH_USED_IN_OP_ACTIVITIES,
            f"{NET_CASH_USED_IN_OP_ACTIVITIES} Reporting date",
            NET_CASH_USED_IN_INVESTING_ACTIVITIES,
            f"{NET_CASH_USED_IN_INVESTING_ACTIVITIES} Reporting date",
            NET_CASH_USED_IN_FINANCING_ACTIVITIES,
            f"{NET_CASH_USED_IN_FINANCING_ACTIVITIES} Reporting date",
            CASH_EQUIVALENTS,
            f"{CASH_EQUIVALENTS} Reporting date",
            INCOME_LOSS_BEFORE_CONT_OPS,
            f"{INCOME_LOSS_BEFORE_CONT_OPS} Reporting date",
            GROSS_PROFIT,
            f"{GROSS_PROFIT} Reporting date",
            REVENUES,
            f"{REVENUES} Reporting date",
            COST_OF_GOODS_AND_SERVICES_SOLD,
            f"{COST_OF_GOODS_AND_SERVICES_SOLD} Reporting date",
            ASSETS,
            f"{ASSETS} Reporting date",
            COMPREHENSIVE_INCOME_NET_OF_TAX,
            f"{COMPREHENSIVE_INCOME_NET_OF_TAX} Reporting date",
            CASH_EQUIVALENTS_PERIOD_INCREASE_DECREASE,
            f"{CASH_EQUIVALENTS_PERIOD_INCREASE_DECREASE} Reporting date",
        ]

        self.not_financial_columns = ["cik_number", "companyName", "agreementDate", "endDate", "diffInDays"]

    def recursive_lookup(self, data, key):
        """
        Generic function to recursively lookup a key in a dictionary
        :param data: dictionary to lookup
        :param key: key to lookup
        """
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

        """
        Given an excel file, return the columns with CIK number and Agreement Date and Company Name
        :param excel_file: excel file with the data
        :param is_licensee: True if the excel file contains licensee data, False otherwise
        """

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
        # strip company name
        df["Company Name"] = df["Company Name"].str.strip()

        # df["CIK Number"] = df["CIK Number"].astype(int)
        print(f"Columns in the dataframe: {df.columns}")
        return df

    def write_to_file(self, file_name, info):
        """
        Write the info to the file
        :param file_name: name of the file
        :param info: info to be written to the file
        """
        # write the info to the file
        with open(file_name, "a") as f:
            f.write(info + "\n")

    def parse_export_data_to_csv(self, json_data: dict, file_path: str):
        """
        Parse the json data and export it to a csv file
        JSON data is parsed with given KPI variables & agreement date with units
        :param json_data: json data to be parsed
        :param file_path: path to the csv file
        """
        # read json file
        parsed_data = {}

        # # dump json_data to a file
        # with open("raw_company_data.json", "w") as f:
        #     json.dump(json_data, f, indent=4)

        for k, v in json_data.items():
            for kpi_var in self.kpi_variables:
                if kpi_var in json_data[k]:
                    try:
                        unit = list(json_data[k][kpi_var][0]["units"].keys())[0]
                    except Exception as e:
                        print(f"Error on parsing(unit): {e}")
                        print(f"CIK number (value): {k}")
                        continue

                    try:
                        value = json_data[k][kpi_var][0]["units"][unit]
                    except Exception as e:
                        print(f"Error on parsing (value): {e}")
                        print(f"CIK number (value): {k}")
                        continue
                    # value : will contain information about instances of the file
                    # first: sort all instances inside the array object
                    # then check from beginning to
                    sorted_values = sorted(value, key=lambda x: datetime.strptime(x["end"], "%Y-%m-%d"))
                    for i in sorted_values:
                        if i["form"] == "10-K":
                            agreement_date = json_data[k]["agreementDate"]

                            # convert agreement date to datetime object
                            agreement_date = datetime.strptime(agreement_date, "%Y-%m-%d")
                            end_date = datetime.strptime(i["end"], "%Y-%m-%d")

                            # subtract the two dates

                            # "agreementDate": "2019-08-31",
                            # "endDate": "2011-01-02"

                            # if end date is greater than agreement date, then the difference is negative

                            if end_date > agreement_date:
                                diff = end_date - agreement_date
                            else:
                                diff = agreement_date - end_date

                            if "diffInDays" not in json_data[k]:
                                json_data[k]["diffInDays"] = abs(diff.days)
                            elif "diffInDays" in json_data[k]:
                                if abs(diff.days) < json_data[k]["diffInDays"]:
                                    json_data[k]["diffInDays"] = abs(diff.days)

                            if abs(diff.days) <= json_data[k]["diffInDays"]:
                                json_data[k]["diffInDays"] = abs(diff.days)
                                val = i["val"]
                                company_name = json_data[k]["entityName"]
                                form_type = i["form"]

                                if k not in parsed_data:
                                    parsed_data[k] = {}

                                if company_name not in parsed_data[k]:
                                    parsed_data[k]["companyName"] = company_name

                                if kpi_var not in parsed_data[k]:
                                    parsed_data[k][kpi_var] = {}

                                parsed_data[k][kpi_var]["value"] = val
                                parsed_data[k][kpi_var]["filedDate"] = i["filed"]
                                parsed_data[k][kpi_var]["endDate"] = i["end"]
                                parsed_data[k][kpi_var]["unit"] = unit
                                parsed_data[k][kpi_var]["form"] = form_type
                                parsed_data[k]["diffInDays"] = diff.days
                                parsed_data[k]["agreementDate"] = json_data[k]["agreementDate"]
                                parsed_data[k]["endDate"] = i["end"]

        # with open("parsed_data.json", "w") as f:
        #     json.dump(parsed_data, f, indent=4)

        self.export_to_csv_file(parsed_data, file_path)

    def export_to_csv_file(self, parsed_data: dict, output_file: str):
        """
        Export the parsed data to a csv file
        :param parsed_data: parsed data
        :param output_file: output file name
        """
        data = ""
        delimeter = ";"
        # add reporting date column after each column name
        # for instance like this Revenues    |      Revenues reporting date        |       GrossProfit       |        GrossProfit reporting date etc

        all_header_info = self.not_financial_columns + self.kpi_variables

        with open(output_file, "w") as f:
            writer = csv.writer(f, delimiter=delimeter)
            writer.writerow(all_header_info)
            cleaned_kpi_variables = [i for i in self.kpi_variables if not i.endswith("Reporting date")]
            for k, v in parsed_data.items():
                data += str(k) + delimeter
                for i in self.not_financial_columns[1:]:
                    if i in parsed_data[k]:
                        data += str(parsed_data[k][i]) + delimeter
                    else:
                        data += "NAN" + delimeter
                for i in cleaned_kpi_variables:
                    if i in parsed_data[k]:
                        data += str(parsed_data[k][i]["value"]) + delimeter
                        reporting_date = parsed_data[k][i]["filedDate"]
                        data += reporting_date + delimeter
                    else:
                        if i == GROSS_PROFIT:
                            if REVENUES in parsed_data[k] and COST_OF_GOODS_AND_SERVICES_SOLD in parsed_data[k]:
                                revenues = parsed_data[k][REVENUES]["value"]
                                cost_of_goods_and_services = parsed_data[k][COST_OF_GOODS_AND_SERVICES_SOLD]["value"]
                                cn = parsed_data[k]["companyName"]
                                print(
                                    f"{cn} GrossProfit is generated from Revenues and Cost of goods and services sold"
                                )
                                gross_profit = float(revenues) - float(cost_of_goods_and_services)
                                data += str(gross_profit) + delimeter
                                # todo: check here
                                reporting_date = parsed_data[k][REVENUES]["filedDate"]
                                data += reporting_date + delimeter
                        else:
                            data += "NAN" + delimeter
                            reporting_date = "NAN" + delimeter
                            data += reporting_date
                f.write(data + "\n")
                data = ""
        print(f"Exported to {output_file}")

    async def get_company_raw_data(self, company_name, cik_number):
        """
        Retrieve the raw company data from the SEC.gov website

        :param company_name: Name of the company
        :param cik_number: CIK number of the company
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number}.json"
            async with session.get(url) as response:
                if response.status == 200:
                    json_data = await response.json()
                    return json_data
                else:
                    print(
                        f"No response: [  {company_name} | {cik_number} | reason: {response.reason} | status: {response.status} ]"
                    )
                    timestamp = datetime.now().strftime("%d_%m_%Y")
                    self.write_to_file(
                        file_name=os.path.join(os.path.abspath("data"), f"no_response_{timestamp}.txt"),
                        # info=f"{company_name}, {cik_number}"
                        info=f"company: {company_name} | cik: {cik_number} | status: {response.status} | reason: {response.reason}\n \t ---> {url}",
                    )
                    return None

    async def get_company_facts_data(self, df):
        """
        Retrieve the company facts data asynchronously

        :param df: Dataframe containing the company names and CIK numbers
        """
        tasks = []
        results = {}
        for index, row in df.iterrows():
            cik_number = row["CIK Number"]
            company_name = row["Company Name"]
            agreement_date = row["Agreement Date"]
            tasks.append(asyncio.ensure_future(self.process_company(cik_number, company_name, agreement_date)))
            if len(tasks) == 10:
                results.update(await self.run_parallel_requests(tasks))
                tasks = []

        if tasks:
            results.update(await self.run_parallel_requests(tasks))

        print(f"Total number of companies: {len(results)}")
        return results

    async def process_company(self, cik_number, company_name, agreement_date):
        """
        Process the company data and retrieve the KPI data asynchronously
        :param cik_number: CIK number of the company
        :param company_name: Name of the company
        :param agreement_date: License agreement date of the company
        """
        company_facts_data = await self.get_company_raw_data(company_name, cik_number)
        if company_facts_data:
            kpi_data = {}
            # remove all elements which ends with "Reporting date"
            revisioned_kpi_vars = [i for i in self.kpi_variables if not i.endswith("Reporting date")]
            for kpi_var in revisioned_kpi_vars:
                kpi_information = list(self.recursive_lookup(company_facts_data, kpi_var))
                if kpi_information:
                    kpi_data[kpi_var] = kpi_information
                else:
                    timestamp = datetime.now().strftime("%d_%m_%Y")
                    self.write_to_file(
                        file_name=os.path.join(os.path.abspath("data"), f"missing_kpi_vars_{timestamp}.txt"),
                        info=f"{company_name} | {cik_number} | {kpi_var}",
                    )
            return (cik_number, agreement_date, company_facts_data["entityName"], kpi_data)

    async def run_parallel_requests(self, tasks):
        """
        Run the parallel requests
        :param tasks: List of tasks to run
        """
        results = {}
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for result in await asyncio.gather(*tasks):
                if result is not None:
                    cik_number, agreement_date, entity_name, kpi_data = result
                    if cik_number not in results:
                        results[cik_number] = {}
                    results[cik_number]["agreementDate"] = str(agreement_date.date())
                    results[cik_number]["entityName"] = entity_name
                    results[cik_number].update(kpi_data)
                await asyncio.sleep(0.1)  # 10 requests per second max
        return results

    def get_data_from_sec_gov_in_parallel(
        self,
        url,
        company_name,
        results,
    ):
        """ "
        Get the data from the SEC.gov website in parallel
        :param url: URL to get the data from
        :param company_name: Name of the company
        :param results: Results dictionary
        """

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
            except KeyError:
                street1 = "No street1 found"

            try:
                street2 = result["addresses"]["business"]["street2"]
            except KeyError:
                street2 = "No street2 found"

            try:
                city = result["addresses"]["business"]["city"]
            except KeyError:
                city = "No city found"

            try:
                state = result["addresses"]["business"]["state"]
            except KeyError:
                state = "No state found"

            try:
                zipCode = result["addresses"]["business"]["zipCode"]
            except KeyError:
                zipCode = "No zipCode found"

            try:
                stateOrCountry = result["addresses"]["business"]["stateOrCountry"]
            except KeyError:
                stateOrCountry = "No stateOrCountry found"

            try:
                stateOrCountryDescription = result["addresses"]["business"]["stateOrCountryDescription"]
            except KeyError:
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
            except KeyError:
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


async def main():
    # create the crawler

    timestamp = datetime.now().strftime("%d_%m_%Y")
    # write a argument parser
    parser = argparse.ArgumentParser(description="Get the company facts from the SEC.gov website")
    parser.add_argument(
        "--source_file",
        type=str,
        help="path to the input file",
        required=True,
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=f"company_facts_{timestamp}_big.csv",
        help="path to the output file",
        required=False,
    )
    parser.add_argument(
        "--is_licensee",
        type=bool,
        help="boolean value to indicate if the source file is for licensee or licensor",
        required=True,
    )
    args = parser.parse_args()
    source_file = args.source_file
    output_file = args.output_file
    is_licensee = args.is_licensee

    if not os.path.exists(source_file):
        raise Exception("The source file does not exist")

    crawler = Crawler()
    fy_cik_df = crawler.get_cik_number_fy_columns(source_file, is_licensee=is_licensee)
    company_info = await crawler.get_company_facts_data(fy_cik_df)

    # save company facts
    crawler.parse_export_data_to_csv(company_info, output_file)


asyncio.run(main())
# In case of running only crawler part
# ----------------------------------------------------------------------------------------------------------------------------
# if __name__ == "__main__":
# print(os.path.dirname(os.path.abspath("data")))
# data_path = os.path.join("sample_data.xlsx")
# timestamp = datetime.now().strftime("%d_%m_%Y")
# # crawler.find_publication("GB2419368")
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
