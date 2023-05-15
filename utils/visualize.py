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

import matplotlib.pyplot as plt
import os


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


if __name__ == "__main__":
    file_path = "./data/not_matched_companies.txt"
    lines = read_file(file_path)
    mapping = create_distribution_mapping(lines)
    print(mapping)
    plot_distribution_mapping(mapping, is_licensee=False)
