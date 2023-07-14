# author: mrtrkmn@github
# description: This file contains several functions to visualize data from a given not_matched_companies.txt file
# the input file should contain only list of companies with a new line e.g

#       Aiwuan Shanghai Ltd.
#       View-Master Ideal Group
#       GCA Therapeutics Ltd.
#       LYZZ Alpha Holding Ltd.
#       Nordic White Diesel AS
#       La Societe Pulsalys
#       BBD Enterprises W.L.L.

import json
import os
import sys
from argparse import ArgumentParser

import matplotlib.pyplot as plt


# Read file
def read_file(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
    return lines


# create distribution mapping
def create_distribution_mapping(lines):
    mapping = {}
    if "Inventor" not in mapping:
        mapping["Inventor"] = 0
    if "Unknown" not in mapping:
        mapping["Unknown"] = 0
    if "University" not in mapping:
        mapping["University"] = 0
    if "Research" not in mapping:
        mapping["Research"] = 0
    if "Institut" not in mapping:
        mapping["Institut"] = 0
    if "Others" not in mapping:
        mapping["Others"] = 0

    for i in lines:
        if i.startswith("Inventor"):
            mapping["Inventor"] += 1
        elif i.startswith("Unknown"):
            mapping["Unknown"] += 1
        elif "University" in i or "School" in i:
            mapping["University"] += 1
        elif "Research" in i or "Development" in i:
            mapping["Research"] += 1
        elif "Institut" in i:
            mapping["Institut"] += 1
        else:
            mapping["Others"] += 1
    return mapping


# plot distribution mapping
def plot_distribution_mapping(mapping, is_licensee):
    labels = list(mapping.keys())
    values = list(mapping.values())
    if is_licensee:
        plot_title = "Distribution of not matched companies [Licensee]"
    else:
        plot_title = "Distribution of not matched companies [Licensor]"
    # write counts on top of bars
    for i in range(len(values)):
        plt.text(i, values[i], values[i], ha="center")

    plt.bar(labels, values)
    plt.title(plot_title)
    plt.xlabel("Category")
    plt.ylabel("Count")
    plt.show()


def create_distribution_mapping_from_json(json_file_path, top_n=10):
    """Creates bar plot of distribution of missing kpi vars"""
    with open(json_file_path, "r") as f:
        data = json.load(f)

    companies = list(data.keys())
    missing_kpi_counts = [data[company]["number_of_missing_kpi_vars"] for company in companies]

    # Sort the companies based on the number of missing KPI variables in descending order
    sorted_indices = sorted(range(len(missing_kpi_counts)), key=lambda k: missing_kpi_counts[k], reverse=True)
    # top_10_companies = [companies[i] for i in sorted_indices[:10]]
    # top_10_missing_kpi_counts = [missing_kpi_counts[i] for i in sorted_indices[:10]]
    top_n_companies = [companies[i] for i in sorted_indices[:top_n]]
    top_n_missing_kpi_counts = [missing_kpi_counts[i] for i in sorted_indices[:top_n]]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(range(len(top_n_companies)), top_n_missing_kpi_counts)
    ax.set_xlabel("Company")
    ax.set_ylabel("Number of Missing KPI Variables")
    if "licensee" in json_file_path:
        ax.set_title(f"Top {top_n} Companies with Highest Number of Missing KPI Variables [Licensee]")
    else:
        ax.set_title("Top 10 Companies with Highest Number of Missing KPI Variables [Licensor]")
    ax.set_xticks(range(len(top_n_companies)))
    ax.set_xticklabels(top_n_companies, rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    """
    Example usage:
    python3 visualize.py --not_found_companies  ./data/not_matched_companies.txt
    python3 visualize.py --missing_kpi_vars ./data/missing_kpi_vars_licensee.json --top_n 10
    """
    parser = ArgumentParser(
        description="""
        This tool contains several helper functions to visualize data from a given not_matched_companies.txt file and missing_kpi_vars.json file
        """,
        add_help=True,
    )
    parser.add_argument(
        "--not_found_companies",
        type=str,
        help="Path to not found companies file [TXT] *(should only contain a list of companies)",
    )
    parser.add_argument("--missing_kpi_vars", type=str, help="Path to missing KPI vars file [JSON]")
    parser.add_argument("--top_n", type=int, help="Number of top companies to show in the bar plot")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    if args.not_found_companies:
        file_path = args.not_found_companies
        lines = read_file(file_path)
        mapping = create_distribution_mapping(lines)
        print(mapping)
        plot_distribution_mapping(mapping, is_licensee=False)
    else:
        json_file_path = args.missing_kpi_vars
        create_distribution_mapping_from_json(json_file_path, args.top_n)
