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
sys.path.append("orbi")

from variables import SEC_DATA_HEADERS


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


def aggregate_orbis_sec_data(orbis_data_file_path, sec_data_file_path):
    """
    Aggregates orbis and sec data
    :param orbis_data_file_path: path to orbis data file
    :param sec_data_file_path: path to sec data file
    """

    if not os.path.exists(orbis_data_file_path):
        raise Exception(f"Orbis output file does not exist at {orbis_data_file_path}")

    if not os.path.exists(sec_data_file_path):
        raise Exception(f"SEC output file does not exist at {sec_data_file_path}")

    orbis_data = pd.read_excel(orbis_output_file, sheet_name="Results")

    # in case one column has multiple values, remove the values after the dot
    columns_to_delete = orbis_data.filter(regex="\.\d+$").columns
    orbis_data = orbis_data.drop(columns=columns_to_delete)
    orbis_data["Company name Latin alphabet"] = orbis_data["Company name Latin alphabet"].str.upper()
    orbis_data.dropna(subset=["Company name Latin alphabet"], inplace=True)

    sec_data = pd.read_csv(sec_output_file, delimiter=";", usecols=SEC_DATA_HEADERS)
    # change column name to match with orbis data
    sec_data.rename(columns={"companyName": "Company name Latin alphabet"}, inplace=True)
    # make all company names uppercase
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.upper()
    # remove /DE from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/DE", "")
    # remove /NEW/ from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/NEW/", "")

    merged_df = merge_dataframes_on(orbis_data, sec_data, "Company name Latin alphabet")

    # drop Unnamed: 0
    merged_df.drop(columns=["Unnamed: 0"], inplace=True)

    # add entry column from raw input excel file to the merged excel file
    add_entry_column(merged_df, sec_data)
    merged_df = duplicate_values_companies(merged_df)
    # Save the updated DataFrame to the Excel file
    write_to_excel(merged_df, merged_output_file)

    add_df_as_another_sheet(sec_data, merged_output_file, "SEC-API-DATA")


def create_company_dictionary(file_path, is_licensee=True):
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
        company_name_columns = [
            col for col in df.columns if col.startswith("Licensee") and col.endswith("cleaned") and "CIK" not in col
        ]
    else:
        company_name_columns = [
            col for col in df.columns if col.startswith("Licensor") and col.endswith("cleaned") and "CIK" not in col
        ]
    # Create an empty dictionary
    company_dict = {}

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        entry = row["Entry"]

        # Iterate over each company name column
        for column in company_name_columns:
            if not pd.isnull(row[column]):
                company_name = row[column].strip().upper()  # Remove any leading/trailing whitespace

                # Check if the company name is not empty
                if company_name:
                    # Check if the company name already exists in the dictionary
                    if company_name in company_dict:
                        # Append the entry value to the existing list of values for the company
                        company_dict[company_name].append(entry)
                    else:
                        # Create a new entry in the dictionary with the company name as the key
                        company_dict[company_name] = [entry]

    return company_dict


def add_entry_column(merged_df, sec_data):
    """
    Add a new 'Entry' column to an Excel file with values from the provided company dictionary.

    Args:
        file_path (str): The path to the Excel file.
        company_dict (dict): A dictionary where company names are keys and the values are lists of corresponding entry values.
    """
    # Read the Excel file
    # df = pd.read_excel(file_path)
    entry_company_mapping = create_company_dictionary(
        searched_raw_input_file, is_licensee="licensee" in merged_output_file.lower()
    )
    df = merged_df
    # Create a new column 'Entry' and initialize it with NaN values
    df.insert(0, "Entry", float("nan"))

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        company_name = (
            row["Company name Latin alphabet"].strip().upper()
        )  # Assuming 'Company Name' is the existing column with dictionary keys
        try:
            entry_values = entry_company_mapping[company_name]
            entry_string = ", ".join(str(value) for value in entry_values)
            df.at[index, "Entry"] = entry_string
        except KeyError as ke:
            print(f"KeyError is {ke}")
            pass


def duplicate_values_companies(df):
    """
    Duplicate rows in an Excel file based on the values in the 'Entry' column.
    :param df: merged dataframe
    """
    # Create an empty list to store the duplicated rows
    duplicated_rows = []
    # df = pd.read_csv(merged_file, sep=";")

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        entries = str(row["Entry"]).split(", ")  # Split the entries by comma and space
        print(entries)
        for entry in entries:
            # Create a new row with the duplicated entry and company name
            duplicated_row = row.copy()
            duplicated_row["Entry"] = entry
            duplicated_rows.append(duplicated_row)

        # Remove the processed entry from the Entry column
        row["Entry"] = ""

    # Create a new DataFrame from the duplicated rows
    duplicated_df = pd.DataFrame(duplicated_rows, columns=df.columns)

    return duplicated_df


# create main function init
if __name__ == "__main__":
    parser = ArgumentParser(
        description="Usage: python3 merge.py --orbis_output_file <path_to_orbis_output_file> --sec_output_file <path_to_sec_output_file> --merged_output_file <path_to_merged_output_file>"
    )
    parser.add_argument("--orbis_output_file", type=str, help="Path to orbis output file")
    parser.add_argument("--sec_output_file", type=str, help="Path to sec output file")
    parser.add_argument("--merged_output_file", type=str, help="Path to output file")
    parser.add_argument("--searched_raw_input_file", type=str, help="Path to searched raw excel input file")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    orbis_output_file = args.orbis_output_file
    sec_output_file = args.sec_output_file
    merged_output_file = args.merged_output_file
    searched_raw_input_file = args.searched_raw_input_file

    aggregate_orbis_sec_data(orbis_output_file, sec_output_file)
