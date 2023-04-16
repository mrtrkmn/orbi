import json
import csv
import os
from datetime import datetime, timedelta
import collections

DATA_SOURCE = "/Users/mrtrkmn/Documents/projects/idp-works/data/sec-data-source.json"

DATA_TO_BE_FETCHED = [
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
]

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


kpi_variables = [
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


def parse_json(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)
    return data


# commmon function to receive information based on
def get_company_info(data, key):
    us_gaap = data["facts"]["us-gaap"]
    try:
        info = us_gaap[f"{key}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Key error: {ke}")
        info = -1
    return info


def get_number_of_employees(data):
    """
    Returns list of employee numbers based on form 10Q / 10K
    """
    try:
        # todo: double check this entrance for different companies as well
        # handle if there is an issue
        number_of_employees = data["facts"]["dei"][f"{NUMBER_OF_EMPLOYEES}"]["units"]["employee"]
    except KeyError as ke:
        print(f"EntityNumberOfEmployees key error: {ke}")
        number_of_employees = -1

    return number_of_employees


def get_operating_income_loss(data):
    us_gaap = data["facts"]["us-gaap"]

    try:
        operating_income_loss = us_gaap[f"{OPERATING_INCOME_LOSS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"OperatingIncomeLoss key err {ke}")
        operating_income_loss = -1
    return operating_income_loss


def get_net_income_loss(data):
    us_gaap = data["facts"]["us-gaap"]

    try:
        net_income_loss = us_gaap[f"{NET_INCOME_LOSS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"NetIncomeLoss key err {ke}")
        net_income_loss = -1

    return net_income_loss


def get_net_cash_used_in_op_activities(data):
    "NetCashProvidedByUsedInOperatingActivities"
    us_gaap = data["facts"]["us-gaap"]

    try:
        net_cash_used_in_op_activities = us_gaap[f"{NET_CASH_USED_IN_OP_ACTIVITIES}"]["units"]["USD"]
    except KeyError as ke:
        print(f"NetCashProvidedByUsedInOperatingActivities key err {ke}")
        net_cash_used_in_op_activities = -1

    return net_cash_used_in_op_activities


def get_net_cash_used_in_investing_activities(data):
    us_gaap = data["facts"]["us-gaap"]

    try:
        net_cash_used_in_investing_activities = us_gaap[f"{NET_CASH_USED_IN_INVESTING_ACTIVITIES}"]["units"]["USD"]
    except KeyError as ke:
        print(f"NetCashProvidedByUsedInInvestingActivities key err {ke}")
        net_cash_used_in_investing_activities = -1

    return net_cash_used_in_investing_activities


def get_net_cash_used_in_financing_activities(data):
    us_gaap = data["facts"]["us-gaap"]

    try:
        net_cash_used_in_financing_activities = us_gaap[f"{NET_CASH_USED_IN_FINANCING_ACTIVITIES}"]["units"]["USD"]
    except KeyError as ke:
        print(f"NetCashProvidedByUsedInFinancingActivities key err {ke}")
        net_cash_used_in_financing_activities = -1

    return net_cash_used_in_financing_activities


def get_cash_equivalents(data):
    us_gaap = data["facts"]["us-gaap"]

    try:
        cash_equivalents = us_gaap[f"{CASH_EQUIVALENTS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Key err {ke}")
        cash_equivalents = -1

    return cash_equivalents


def get_assets(data):
    us_gaap = data["facts"]["us-gaap"]
    try:
        assets = us_gaap[f"{ASSETS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Assets key err {ke}")
        assets = -1

    return assets


def get_net_income_loss(data):
    us_gaap = data["facts"]["us-gaap"]
    try:
        net_income_loss = us_gaap[f"{NET_INCOME_LOSS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Key err {ke}")
        net_income_loss = -1

    return net_income_loss


def get_income_loss_before_cont_ops(data):
    us_gaap = data["facts"]["us-gaap"]
    try:
        income_loss_before_cont_ops = us_gaap[f"{INCOME_LOSS_BEFORE_CONT_OPS}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Key error {ke}")
        income_loss_before_cont_ops = -1
    return income_loss_before_cont_ops


def get_gross_profit(data):
    us_gaap = data["facts"]["us-gaap"]
    try:
        gross_profit = us_gaap[f"{GROSS_PROFIT}"]["units"]["USD"]
    except KeyError as ke:
        print(f"Key error {ke}")
        gross_profit = -1
    return gross_profit


def recursive_lookup(data, key):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                yield v
            yield from recursive_lookup(v, key)
    elif isinstance(data, list):
        for item in data:
            yield from recursive_lookup(item, key)


def create_csv_file(headers, row, file_name):
    if os.path.exists(file_name):
        with open(file_name, "a") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    else:
        with open(file_name, "w") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def parse_data_from_end_result(file_path: str):
    # read json file
    parsed_data = {}

    with open(file_path, "r") as f:
        json_data = json.load(f)
    # parse the data
    for k, v in json_data.items():
        for kpi_var in kpi_variables:
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

                        # write to txt file

                        # if diff <= -1:
                        #     with open("company_facts_14_04_2023.txt", "a") as f:
                        #         f.write(f"Company Name: {company_name}, {kpi_var}: {val}, Unit: {unit} , Form: {form_type} , FY: {i['fy']} Start: {i['start']} End:{i['end']} Filed: {i['filed']}" + "\n")

                        # print(f"Company Name: {company_name}, {kpi_var}: {val}, Unit: {unit} , Form: {form_type} , FY: {i['fy']} Start: {i['start']} End:{i['end']} Filed: {i['filed']}")
                # break
            except KeyError as ke:
                # print(f"Key error: {ke}")
                # print(f"Key: {kpi_var}")
                # print(f"File: {file_path}")
                continue
    return parsed_data


def json_to_csv(input_file: str, output_file: str):
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


timestamp = datetime.now().strftime("%d_%m_%Y")
company_facts_file_name = f"company_facts_{timestamp}.json"

company_facts_file_name_parsed = f"company_facts_{timestamp}_parsed.json"
company_facts_file_name_parsed_csv = f"company_facts_{timestamp}_parsed.csv"

company_facts_csv_file_name = f"company_facts_{timestamp}.csv"

parsed_data = parse_data_from_end_result(company_facts_file_name)

with open(company_facts_file_name_parsed, "w") as f:
    json.dump(parsed_data, f, indent=4)

json_to_csv(company_facts_file_name_parsed, output_file=company_facts_file_name_parsed_csv)
