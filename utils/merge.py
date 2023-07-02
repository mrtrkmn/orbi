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


def create_bvd_own_id_mapping(orbis_id_export_file):
    data_path = str(os.path.join(root_path, "..", "data"))
    orbis_id_export_file = os.path.join(data_path, orbis_id_export_file)

    # check if file exists or not
    if not os.path.exists(orbis_id_export_file):
        raise Exception(f"Orbis ID export file does not exist at {orbis_id_export_file}")

    print(f"Creating BVD own id mapping... ")
    df_id_export = pd.read_excel(orbis_id_export_file)
    df_id_export = df_id_export[["Matched BvD ID", "Own ID"]]

    # map Matched BvD ID to Own ID
    mapping = {}
    for index, row in df_id_export.iterrows():
        mapping[row["Matched BvD ID"]] = row["Own ID"]

    return mapping


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
    orbis_data_file_path = os.path.abspath(orbis_data_file_path)

    if not os.path.exists(orbis_data_file_path):
        raise Exception(f"Orbis output file does not exist at {orbis_data_file_path}")

    if not os.path.exists(sec_data_file_path):
        raise Exception(f"SEC output file does not exist at {sec_data_file_path}")

    orbis_data = pd.read_excel(orbis_output_file, sheet_name="Results")
    orbis_id_bvd_mapping_file_name = f"Export_{os.path.basename(orbis_data_file_path).split('.')[0]}.xlsx"

    bvd_own_id_mapping = create_bvd_own_id_mapping(orbis_id_bvd_mapping_file_name)

    # in case one column has multiple values, remove the values after the dot
    columns_to_delete = orbis_data.filter(regex="\.\d+$").columns
    orbis_data = orbis_data.drop(columns=columns_to_delete)
    orbis_data["Company name Latin alphabet"] = orbis_data["Company name Latin alphabet"].str.upper()
    orbis_data.dropna(subset=["Company name Latin alphabet"], inplace=True)
    # add Own ID column to first column
    orbis_data.insert(0, "Own ID", "")

    # fill Own ID column with mapping
    for index, row in orbis_data.iterrows():
        orbis_data.at[index, "Own ID"] = bvd_own_id_mapping[row["BvD ID number"]]

    # orbis_data = pd.DataFrame(orbis_data)

    orbis_data.drop(columns=["Unnamed: 0"], inplace=True)
    # replace the coulmn name
    # orbis_data.rename(columns={"Unnamed: 0": "Own ID"}, inplace=True)
    # export the orbis data to excel fil
    orbis_data.to_excel("orbis_data.xlsx", index=False)

    sec_data = pd.read_csv(sec_output_file, delimiter=";", usecols=SEC_DATA_HEADERS)
    # change column name to match with orbis data
    sec_data.rename(columns={"companyName": "Company name Latin alphabet"}, inplace=True)
    sec_data.rename(columns={"entry": "Entry"}, inplace=True)
    # make all company names uppercase
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.upper()
    # remove /DE from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/DE", "")
    # remove /NEW/ from company name
    sec_data["Company name Latin alphabet"] = sec_data["Company name Latin alphabet"].str.replace("/NEW/", "")

    merged_df = duplicate_values_companies(orbis_data)
    # Save the updated DataFrame to the Excel file

    # convert column Own ID to int64
    sec_data["Entry"] = sec_data["Entry"].astype(str)
    merged_df = merge_dataframes_on(merged_df, sec_data, "Entry")
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


def duplicate_values_companies(df):
    """
    Duplicate rows in an Excel file based on the values in the 'Entry' column.
    :param df: merged dataframe
    """
    # Create an empty list to store the duplicated rows
    duplicated_rows = []
    # df = pd.read_csv(merged_file, sep=";")
    columnns = ["Entry"] + list(df.columns)
    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        entries = str(row["Own ID"]).split(", ")  # Split the entries by comma and space
        for entry in entries:
            # Create a new row with the duplicated entry and company name
            duplicated_row = row.copy()
            # drop entry value delete [ ] characters
            duplicated_row["Entry"] = entry.replace("[", "").replace("]", "")
            duplicated_rows.append(duplicated_row)

        # Remove the processed entry from the Entry column
        row["Entry"] = ""

    # Create a new DataFrame from the duplicated rows
    duplicated_df = pd.DataFrame(duplicated_rows, columns=columnns)
    duplicated_df["Entry"] = duplicated_df["Entry"].astype(str)
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
