# author: mrtrkmn@github
# date: 07.06.2023
# desc: This file contains helper functions to merge orbis and sec data

import os
import sys
from argparse import ArgumentParser

import pandas as pd
from fuzzywuzzy import fuzz

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)


def parse_data_based_on_licensee_aggrement_data(excel_path_file):
    """
    This function parses the excel file and returns a dict with key as agreement date and value as list of licensee
    :param excel_path_file: path to excel file
    :return: dict with key as agreement date and value as list of licensee
    """
    df = pd.read_excel(excel_path_file)
    licensee_columns = [
        col for col in df.columns if col.startswith("Licensee") and col.endswith("cleaned") and "CIK" not in col
    ]
    # get all licensee_columns and also agreement date
    licensee_columns.append("Agreement Date")
    df = df[licensee_columns]
    print(df.head())
    # iterate over all rows and get all licensee and agreement date to dict
    licensee_agreement_date_dict = {}
    for index, row in df.iterrows():
        licensee_agreement_date_dict[row["Agreement Date"]] = []
        for licensee_column in licensee_columns[:-1]:
            if row[licensee_column] is not None and row[licensee_column] != "":
                licensee_agreement_date_dict[row["Agreement Date"]].append(row[licensee_column])
    return licensee_agreement_date_dict


def pretty(d, indent=0):
    """
    Pretty print a dict
     :param d: dict
     :param indent: indent
    """
    for key, value in d.items():
        print("\t" * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print("\t" * (indent + 1) + str(value))


def calculate_match_percentage(string1, string2):
    """
    Calculates match percentage of two strings
    :param string1: string 1
    :param string2: string 2
    """
    return fuzz.ratio(string1, string2) / 100.0


def get_ids_of_companies(path_to_file):
    """
    Creates a dictionary with key as company name and value as list of ids
    :param path_to_file: path to excel file
    """

    df = pd.read_excel(path_to_file)
    # id column name is Entry
    # create a dict with list of ids of a company
    company_name_ids = {}
    # example of the dict
    # {company_name: [id1, id2, id3, ...]}
    # combine all
    licensee_columns = [
        col for col in df.columns if col.startswith("Licensee") and col.endswith("cleaned") and "CIK" not in col
    ]
    # get all licensee columns, entry and agreement date
    licensee_columns.append("Entry")
    licensee_columns.append("Agreement Date")
    df = df[licensee_columns]
    # iterate over all rows and get all licensee and agreement date to dict
    for index, row in df.iterrows():
        for licensee_column in licensee_columns[:-2]:
            company_name = row[licensee_column]
            if company_name not in company_name_ids.keys():
                company_name_ids[company_name] = [row["Entry"]]
            else:
                company_name_ids[company_name].append(row["Entry"])
    return company_name_ids


def merge_dataframes_on(df, df_to_merge, on_column, how="left"):
    """
    Merges two dataframes on a column
    :param df: dataframe 1
    :param df_to_merge: dataframe 2
    :param on_column: column to merge on
    :param how: how to merge
    """
    # refer : https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    df = pd.merge(df, df_to_merge, on=on_column, how=how)
    return df


def add_df_as_another_sheet(df, file_name, sheet_name):
    """
    Adds a dataframe as another sheet to an excel file
    :param df: dataframe
    :param file_name: excel file name
    :param sheet_name: sheet name
    """
    with pd.ExcelWriter(file_name, mode="a") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def write_to_excel(df, file_name, sheet_name="ORBIS-SEC-MERGED"):
    """
    Writes a dataframe to an excel file
    :param df: dataframe
    :param file_name: excel file name
    """
    df.to_excel(file_name, index=False, sheet_name=sheet_name)


# create main function init
if __name__ == "__main__":
    parser = ArgumentParser(
        description="Usage: python3 merge.py --orbis_output_file <path_to_orbis_output_file> --sec_output_file <path_to_sec_output_file> --merged_output_file <path_to_merged_output_file>"
    )
    parser.add_argument("--orbis_output_file", type=str, help="Path to orbis output file")
    parser.add_argument("--sec_output_file", type=str, help="Path to sec output file")
    parser.add_argument("--merged_output_file", type=str, help="Path to output file")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    orbis_output_file = args.orbis_output_file
    sec_output_file = args.sec_output_file
    merged_output_file = args.merged_output_file

    if not os.path.exists(orbis_output_file):
        raise Exception(f"Orbis output file does not exist at {orbis_output_file}")

    if not os.path.exists(sec_output_file):
        raise Exception(f"SEC output file does not exist at {sec_output_file}")

    orbis_data = pd.read_excel(orbis_output_file, sheet_name="Results")
    sec_data = pd.read_csv(sec_output_file, delimiter=";", on_bad_lines="skip")

    # change column name to match with orbis data
    sec_data.rename(columns={"companyName": "Company name Latin alphabet"}, inplace=True)
    # make all company names uppercase
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.upper()
    # remove /DE from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/DE", "")
    # remove /NEW/ from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/NEW/", "")

    # merge dataframes on company name
    merged_df = merge_dataframes_on(orbis_data, sec_data, "Company name Latin alphabet")
    # write to excel
    write_to_excel(merged_df, merged_output_file)
    # add sec data as another sheet to the excel file
    add_df_as_another_sheet(sec_data, merged_output_file, "SEC-API-DATA")
