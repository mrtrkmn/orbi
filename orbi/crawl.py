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
from ast import dump
from datetime import datetime
from threading import Thread

import aiohttp
import pandas as pd
import pytz
import requests
import unidecode
import yaml
from bs4 import BeautifulSoup

# Description: This script crawls the IPO website for the patent-related data and SEC.gov for the company-related data
#             and stores them in a csv file.
# Python Version: 3.9.5
# Python Libraries: requests, bs4, threading, time, pandas, csv, re


# this path append is needed for pdoc to generate the documentation
root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)

LOCAL_TZ = pytz.timezone("Europe/Berlin")
NOT_FOUND_COMPANIES = {}
MISSING_KPI_VARS = {}

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

        self.not_financial_columns = ["entry", "cik_number", "companyName", "agreementDate", "endDate", "diffInDays"]

    def provide_complete_cik_number(self, incomplete_cik_number):
        """
        This function completes incomplete CIK number by appending zeros at the beginning
        :param input_data: incomplete CIK number
        :return: complete CIK number
        """
        if incomplete_cik_number is not None and str(incomplete_cik_number) != "nan":
            # Check if input contains only numbers
            if str(incomplete_cik_number).isdigit():
                # Append zeros at the beginning to make it a 10-digit number
                return str(incomplete_cik_number).zfill(10)
        else:
            return None

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

    def get_dict_value(self, data, cik_number, key):
        company_data = data[cik_number]
        if isinstance(company_data, dict):
            for k, v in company_data.items():
                if k == key:
                    return v
                else:
                    return self.get_dict_value(v, key)
        elif isinstance(company_data, list):
            for item in company_data:
                return self.get_dict_value(item, key)

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

    def get_cik_number_fy_columns(self, excel_file, is_licensee):
        # get Licensee CIK 1_cleaned and Agreement Date
        # substract one year from the Agreement Date

        """
        Given an excel file, return the columns with CIK number and Agreement Date and Company Name
        :param excel_file: excel file with the data
        :param is_licensee: True if the excel file contains licensee data, False otherwise
        """
        complete_df = pd.read_excel(excel_file, dtype=str)
        final_df = pd.DataFrame()
        licensee_columns = [
            col
            for col in complete_df.columns
            if col.startswith("Licensee") and col.endswith("cleaned") and "CIK" not in col
        ]
        licensor_columns = [
            col
            for col in complete_df.columns
            if col.startswith("Licensor") and col.endswith("cleaned") and "CIK" not in col
        ]
        number_of_licensee_columns = len(licensee_columns)
        number_of_licensor_columns = len(licensor_columns)
        ENTRY = "Entry"
        if is_licensee:
            for i in range(0, number_of_licensee_columns):
                LICENSEE_CLEANED_NAME = f"Licensee {i+1}_cleaned"
                LICENSEE_CIK_CLEANED_NAME = f"Licensee CIK {i+1}_cleaned"
                AGREEMENT_DATE_NAME = "Agreement Date"

                # drop nan and invalid CIK Number from complete_df
                complete_df = complete_df.dropna(subset=[LICENSEE_CIK_CLEANED_NAME])
                # drop not valid CIK Number
                # complete_df = complete_df[complete_df[LICENSEE_CIK_CLEANED_NAME].apply(self.check_cik_number_format)]
                # change the type of the column to str
                # exit()
                df = complete_df[[ENTRY, LICENSEE_CLEANED_NAME, LICENSEE_CIK_CLEANED_NAME, AGREEMENT_DATE_NAME]]
                df[AGREEMENT_DATE_NAME] = pd.to_datetime(df[AGREEMENT_DATE_NAME])
                # df["Agreement Date"] = df["Agreement Date"].dt.year - 1
                df = df.rename(
                    columns={
                        LICENSEE_CIK_CLEANED_NAME: "CIK Number",
                        LICENSEE_CLEANED_NAME: "Company Name",
                    }
                )
                # convert to str
                # df = df.astype(str)
                # df = df.reset_index(drop=True)
                df = df.sort_values(by=["Company Name"], ascending=True)

                final_df = pd.concat([final_df, df])
        else:
            for i in range(0, number_of_licensor_columns):
                LICENSOR_CLEANED_NAME = f"Licensor {i+1}_cleaned"
                LICENSOR_CIK_CLEANED_NAME = f"Licensor CIK {i+1}_cleaned"
                AGREEMENT_DATE_NAME = "Agreement Date"
                # drop nan and invalid CIK Number from complete_df
                complete_df = complete_df.dropna(subset=[LICENSOR_CIK_CLEANED_NAME])
                # complete_df = complete_df[complete_df[LICENSOR_CIK_CLEANED_NAME].apply(self.check_cik_number_format)]

                # change the type of the column to string
                # complete_df[LICENSOR_CIK_CLEANED_NAME] = complete_df[LICENSOR_CIK_CLEANED_NAME].astype(str)
                df = complete_df[[ENTRY, LICENSOR_CLEANED_NAME, LICENSOR_CIK_CLEANED_NAME, AGREEMENT_DATE_NAME]]
                # apply complete check cik number format to the CIK Number
                df[AGREEMENT_DATE_NAME] = pd.to_datetime(df[AGREEMENT_DATE_NAME])
                df = df.rename(
                    columns={
                        LICENSOR_CIK_CLEANED_NAME: "CIK Number",
                        LICENSOR_CLEANED_NAME: "Company Name",
                    }
                )
                # sort the dataframe by Company Name
                df = df.sort_values(by=["Company Name"], ascending=True)
                final_df = pd.concat([final_df, df])
        return final_df

    def parse_export_data_to_csv(self, json_data: dict, file_path: str):
        """
        Parse the json data and export it to a csv file
        JSON data is parsed with given KPI variables & agreement date with units
        :param json_data: json data to be parsed
        :param file_path: path to the csv file
        """

        parsed_data = {}

        for k, v in json_data.items():
            entry_ids_raw = v.keys()
            entry_ids = [i for i in entry_ids_raw if self.is_str_convertible_to_int(i)]
            for entry_id in entry_ids:
                for kpi_var in self.kpi_variables:
                    if kpi_var in v[entry_id]:
                        try:
                            unit = list(v[entry_id][kpi_var][0]["units"].keys())[0]
                        except Exception as e:
                            print(f"Error on parsing(unit): {e}")
                            print(f"CIK number (value): {k}")
                            continue

                        try:
                            value = v[entry_id][kpi_var][0]["units"][unit]
                            sorted_values = sorted(value, key=lambda x: datetime.strptime(x["end"], "%Y-%m-%d"))
                        except Exception as e:
                            print(f"Error on parsing (value): {e}")
                            print(f"CIK number (value): {k}")
                            continue
                        # value : will contain information about instances of the file
                        # first: sort all instances inside the array object
                        # then check from beginning to

                        for i in sorted_values:
                            if i["form"] == "10-K":
                                agreement_date = v[entry_id]["agreementDate"]

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

                                if "diffInDays" not in v[entry_id]:
                                    v[entry_id]["diffInDays"] = abs(diff.days)
                                elif "diffInDays" in v[entry_id]:
                                    if abs(diff.days) < v[entry_id]["diffInDays"]:
                                        v["diffInDays"] = abs(diff.days)

                                if abs(diff.days) <= v[entry_id]["diffInDays"]:
                                    v[entry_id]["diffInDays"] = abs(diff.days)
                                    val = i["val"]
                                    company_name = v["entityName"]
                                    form_type = i["form"]

                                    if k not in parsed_data:
                                        parsed_data[k] = {}

                                    if company_name not in parsed_data[k]:
                                        parsed_data[k]["companyName"] = company_name

                                    if entry_id not in parsed_data[k]:
                                        parsed_data[k][entry_id] = {}

                                    if kpi_var not in parsed_data[k][entry_id]:
                                        parsed_data[k][entry_id][kpi_var] = {}

                                    parsed_data[k][entry_id][kpi_var]["value"] = val
                                    parsed_data[k][entry_id][kpi_var]["filedDate"] = i["filed"]
                                    parsed_data[k][entry_id][kpi_var]["endDate"] = i["end"]
                                    parsed_data[k][entry_id][kpi_var]["unit"] = unit
                                    parsed_data[k][entry_id][kpi_var]["form"] = form_type
                                    parsed_data[k][entry_id]["diffInDays"] = diff.days
                                    parsed_data[k][entry_id]["agreementDate"] = str(agreement_date)
                                    parsed_data[k][entry_id]["endDate"] = i["end"]

        self.export_to_csv_file(parsed_data, file_path)

    def create_entry_cik_number_mapping(file_path, is_licensee=True):
        """
        Create a dictionary from an Excel file with company names as keys and corresponding entry values as dictionary values.

        Args:
            file_path (str): The path to the Excel file.

        Returns:
            dict: A dictionary where company names are keys and the values are lists of corresponding entry values.
        """

        # If path is not exists raise an exception
        if not os.path.exists(file_path):
            raise Exception(f"File does not exist at {file_path}")

        # Read the Excel file
        df = pd.read_excel(file_path)
        if is_licensee:
            company_cik_numbers = [
                col for col in df.columns if col.startswith("Licensee") and col.endswith("cleaned") and "CIK" in col
            ]
        else:
            company_cik_numbers = [
                col for col in df.columns if col.startswith("Licensor") and col.endswith("cleaned") and "CIK" in col
            ]
        # Create an empty dictionary
        company_dict = {}

        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            entry = row["Entry"]

            # Iterate over each company name column
            for column in company_cik_numbers:
                if not pd.isnull(row[column]):
                    company_name = row[column]
                    if company_name in company_dict:
                        # Append the entry value to the existing list of values for the company
                        company_dict[company_name].append(entry)
                    else:
                        # Create a new entry in the dictionary with the company name as the key
                        company_dict[company_name] = [entry]

        return company_dict

    def is_str_convertible_to_int(self, value: str):
        """
        Check if a string is convertible to int
        :param value: string value
        :return: True if the string is convertible to int, False otherwise
        """
        try:
            int(value)
            return True
        except ValueError:
            return False

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
        output_file = os.path.join(os.path.abspath("data"), output_file)
        with open(output_file, "w") as f:
            writer = csv.writer(f, delimiter=delimeter)
            writer.writerow(all_header_info)
            cleaned_kpi_variables = [i for i in self.kpi_variables if not i.endswith("Reporting date")]
            for k, v in parsed_data.items():
                # get current company entry id
                entry_ids = list(parsed_data[k].keys())
                entry_ids.remove("companyName")

                for entry_id in entry_ids:
                    entry_id = str(entry_id)
                    data += entry_id + delimeter

                    data += str(k) + delimeter
                    company_name = parsed_data[k]["companyName"]
                    agreement_date = parsed_data[k][entry_id]["agreementDate"]
                    end_date = parsed_data[k][entry_id]["endDate"]
                    diff_in_days = parsed_data[k][entry_id]["diffInDays"]

                    data += str(company_name) + delimeter
                    data += str(agreement_date) + delimeter
                    data += str(end_date) + delimeter
                    data += str(diff_in_days) + delimeter

                    for i in cleaned_kpi_variables:
                        if i in parsed_data[k][entry_id]:
                            data += str(parsed_data[k][entry_id][i]["value"]) + delimeter
                            reporting_date = parsed_data[k][entry_id][i]["endDate"]
                            data += reporting_date + delimeter
                        else:
                            if i == GROSS_PROFIT:
                                if (
                                    REVENUES in parsed_data[k][entry_id]
                                    and COST_OF_GOODS_AND_SERVICES_SOLD in parsed_data[k][entry_id]
                                ):
                                    revenues = parsed_data[k][entry_id][REVENUES]["value"]
                                    cost_of_goods_and_services = parsed_data[k][entry_id][
                                        COST_OF_GOODS_AND_SERVICES_SOLD
                                    ]["value"]
                                    cn = parsed_data[k]["companyName"]
                                    print(
                                        f"{cn} GrossProfit is generated from Revenues and Cost of goods and services sold"
                                    )
                                    gross_profit = float(revenues) - float(cost_of_goods_and_services)
                                    data += str(gross_profit) + delimeter
                                    # todo: check here
                                    reporting_date = parsed_data[k][entry_id][REVENUES]["filedDate"]
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
                    if company_name not in NOT_FOUND_COMPANIES:
                        timestamp_with_hour_minute = datetime.now(tz=LOCAL_TZ).strftime("%d_%m_%Y_%H_%M")
                        NOT_FOUND_COMPANIES[company_name] = {
                            "cik": cik_number,
                            "status": response.status,
                            "reason": response.reason,
                            "link": url,
                            "timestamp": timestamp_with_hour_minute,
                        }
                        print(
                            f"No response: [  {company_name} | {cik_number} | reason: {response.reason} | status: {response.status} ]"
                        )

    async def get_company_facts_data(self, df):
        """
        Retrieve the company facts data asynchronously

        :param df: Dataframe containing the company names and CIK numbers
        """
        tasks = []
        results = {}
        for index, row in df.iterrows():
            entry_id = row["Entry"]
            cik_number = row["CIK Number"]
            company_name = row["Company Name"]
            agreement_date = row["Agreement Date"]

            tasks.append(
                asyncio.ensure_future(self.process_company(entry_id, cik_number, company_name, agreement_date))
            )
            if len(tasks) == 10:
                results.update(await self.run_parallel_requests(tasks))
                tasks = []

        if tasks:
            results.update(await self.run_parallel_requests(tasks))

        print(f"Total number of companies: {len(results)}")
        return results

    async def process_company(self, entry_id, cik_number, company_name, agreement_date):
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
                    timestamp_with_hour_minute = datetime.now(tz=LOCAL_TZ).strftime("%d_%m_%Y_%H_%M")

                    if company_name not in MISSING_KPI_VARS:
                        MISSING_KPI_VARS[company_name] = {
                            "cik": cik_number,
                            "kpi_vars": [kpi_var],
                            "timestamp_eu": timestamp_with_hour_minute,
                        }
                    else:
                        if kpi_var not in MISSING_KPI_VARS[company_name]["kpi_vars"]:
                            MISSING_KPI_VARS[company_name]["kpi_vars"].append(kpi_var)
                        print(f"Missing KPI variable: [ {company_name} | {cik_number} | {kpi_var} ]")

                    # self.write_to_file(
                    #     file_name=os.path.join(os.path.abspath("data"), f"missing_kpi_vars_{timestamp}.txt"),
                    #     info=f"{company_name} | {cik_number} | {kpi_var}",
                    # )
            return (entry_id, cik_number, agreement_date, company_facts_data["entityName"], kpi_data)

    async def run_parallel_requests(self, tasks):
        """
        Run the parallel requests
        :param tasks: List of tasks to run
        """
        results = {}
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for result in await asyncio.gather(*tasks):
                if result is not None:
                    entry_id, cik_number, agreement_date, entity_name, kpi_data = result
                    if cik_number not in results:
                        results[cik_number] = {}

                    results[cik_number]["entityName"] = entity_name
                    if str(agreement_date.date()) not in results[cik_number]:
                        results[cik_number][entry_id] = {}
                        results[cik_number][entry_id]["agreementDate"] = str(agreement_date.date())
                    results[cik_number][entry_id].update(kpi_data)
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


def is_full_path(path):
    """
    Check if the path is a full path or not
    :param path: path to the file
    """
    if os.path.isabs(path):
        return True
    return False


def dump_to_json_file(data, file_name):
    """
    Dumps the JSON data to a file
    :param file_name: name of the file
    :param data: JSON variable which contains the data
    """
    full_path_to_file = os.path.join(os.path.abspath("data"), file_name)
    # check file_name is whether a full path or file name

    with open(full_path_to_file, "w") as f:
        json.dump(data, f, indent=4)

    print(f"{file_name} saved to {full_path_to_file}")


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


def is_absolute_path(path):
    """
    Check if the path is a full path or not
    :param path: path to the file
    """
    if os.path.isabs(path):
        return True
    return False


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
        help="path to the output file",
        required=False,
    )
    parser.add_argument(
        "--licensee",
        type=bool,
        help="boolean value to indicate if the source file is for licensee or licensor",
        required=True,
        action=argparse.BooleanOptionalAction,
    )
    # if no arguments are provided, print the help message
    if len(sys.argv) == 1:
        print(
            """
            Example usage:\n
            \t python orbi/crawl.py --source_file sample_data.xlsx --output_file company_facts.csv --licensee  # searching over licensee information 
            \t python orbi/crawl.py --source_file sample_data.xlsx --output_file company_facts.csv --no-licensee # searching over licensor information 
            """
        )
        sys.exit(1)

    args = parser.parse_args()
    output_file = args.output_file
    is_licensee = args.licensee
    if is_absolute_path(args.source_file):
        # if the path is absolute, do nothing
        source_file = args.source_file
    else:
        # if not absolute path, point to the data folder
        source_file = os.path.join(os.path.abspath("data"), args.source_file)

    crawler = Crawler()
    print(f"is licensee information {is_licensee}")
    fy_cik_df = crawler.get_cik_number_fy_columns(source_file, is_licensee=is_licensee)
    company_info = await crawler.get_company_facts_data(fy_cik_df)

    if is_licensee:
        not_found_file_name = f"no_response_licensee_{timestamp}.json"
        missing_kpi_var_file_name = f"missing_kpi_vars_licensee_{timestamp}.json"
        if not args.output_file:
            output_file = f"company_facts_{timestamp}_licensee.csv"
    else:
        not_found_file_name = f"no_response_licensor_{timestamp}.json"
        missing_kpi_var_file_name = f"missing_kpi_vars_licensor_{timestamp}.json"
        if not args.output_file:
            output_file = f"company_facts_{timestamp}_licensor.csv"

    for k, v in MISSING_KPI_VARS.items():
        MISSING_KPI_VARS[k]["number_of_missing_kpi_vars"] = len(v["kpi_vars"])

    dump_to_json_file(data=NOT_FOUND_COMPANIES, file_name=not_found_file_name)
    dump_to_json_file(data=MISSING_KPI_VARS, file_name=missing_kpi_var_file_name)

    # save company facts
    crawler.parse_export_data_to_csv(company_info, output_file)


if __name__ == "__main__":
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
