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


# Read file
def read_file(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()
    return lines


# count values which starts with Inventor
def count_inventor(lines):
    """
    Count values which starts with Inventor
    """
    count = 0
    for line in lines:
        if line.startswith("Inventor"):
            count += 1
    return count


# count values which starts with Unknown
def count_unknown(lines):
    """
    Count values which starts with Unknown
    """

    count = 0
    for line in lines:
        if line.startswith("Unknown"):
            count += 1
    return count


# count values which contains 'University'
def count_university(lines):
    count = 0
    for line in lines:
        if "School" in line or "University" in line:
            count += 1
    return count


# count values which contains 'Research or Development'
def count_research(lines):
    count = 0
    for line in lines:
        if "Research" in line or "Development" in line:
            count += 1
    return count


def count_others(lines):
    count = 0
    for line in lines:
        if (
            not line.startswith("Inventor")
            and not line.startswith("Unknown")
            and "University" not in line
            and "Research" not in line
            and "Development" not in line
        ):
            print(line)
            count += 1
    return count


def count_institute(lines):
    count = 0
    for line in lines:
        if "Institut" in line:
            count += 1
    return count


# create distribution mapping
def create_distribution_mapping(lines):
    mapping = {}
    for line in lines:
        if line.startswith("Inventor"):
            mapping["Inventor"] = count_inventor(lines)
        elif line.startswith("Unknown"):
            mapping["Unknown"] = count_unknown(lines)
        elif "University" in line:
            mapping["University"] = count_university(lines)
        elif "Research" in line or "Development" in line:
            mapping["Research"] = count_research(lines)
        elif "Institut" in line:
            mapping["Institut"] = count_institute(lines)
        else:
            mapping["Others"] = count_others(lines)
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
    plot_distribution_mapping(mapping)
