import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.internals.blocks import F

# example run on Google colab
# https://colab.research.google.com/drive/17Zn2y9QnjWchwNdBpJ4brjj7IPzH5I_5?usp=sharing

KPI_COLUMNS = [
    "Operating revenue (Turnover)\nm USD",
    "Sales\nm USD",
    "Gross profit\nm USD",
    "P/L before tax\nm USD",
    "P/L for period [=Net income]\nm USD",
    "Cash flow\nm USD",
    "Total assets\nm USD",
    # "Number of employees\n",
]
available_years = [str(year) for year in range(1994, 2024)]


def create_data_dictionary(df):
    company_missing_kpi_mapping = {}
    # # Iterate over each row
    for index, row in df.iterrows():
        agreement_date_year = str(row["agreementDate"]).split("-")[0]
        if agreement_date_year != "nan":
            agreement_date_year = str(int(agreement_date_year) - 1)
            company_name = row["Company name Latin alphabet_x"]
            if company_name not in d:
                company_missing_kpi_mapping[company_name] = {}
            else:
                if "missing_kpi" not in company_missing_kpi_mapping[company_name]:
                    company_missing_kpi_mapping[company_name]["missing_kpi"] = 0
                else:
                    for column in KPI_COLUMNS:
                        if int(agreement_date_year) < 1994:
                            company_missing_kpi_mapping[company_name][f"{column} {agreement_date_year}"] = False
                        else:
                            print(row[f"{column} {agreement_date_year}"])
                            if (
                                row[f"{column} {agreement_date_year}"] == "n.a."
                                or row[f"{column} {agreement_date_year}"] is None
                            ):
                                company_missing_kpi_mapping[company_name]["missing_kpi"] += 1
                                company_missing_kpi_mapping[company_name][f"{column} {agreement_date_year}"] = False
    return company_missing_kpi_mapping


def create_missing_kpi_var_map(data: dict, is_licensee: bool = True):
    # Extracting companies with missing_kpi values
    companies = [company for company in data.keys() if data[company].get("missing_kpi", 0) > 0]
    missing_kpis = [data[company]["missing_kpi"] for company in companies]

    # Sorting companies based on missing_kpi values (high to low)
    sorted_companies, sorted_missing_kpis = zip(*sorted(zip(companies, missing_kpis), key=lambda x: x[1], reverse=True))

    # Prepare the total number of missing KPI variables for each company
    missing_kpi_vars = []
    for company in sorted_companies:
        missing_vars = [var for var in data[company] if data[company][var] is False]
        missing_kpi_vars.append(", ".join(missing_vars))

    # Plotting the bar chart
    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(sorted_companies)), sorted_missing_kpis)
    plt.xlabel("Company")
    plt.ylabel("Number of Missing KPIs")
    if is_licensee:
        plt.title("Companies with Missing KPIs on merged data (Licensee)")
    else:
        plt.title("Companies with Missing KPIs on merged data (Licensor)")
    plt.xticks(range(len(sorted_companies)), sorted_companies, rotation=90)

    # Adding values inside the bars
    for bar, value in zip(bars, sorted_missing_kpis):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() - 1,
            str(value),
            ha="center",
            va="center",
            fontsize=10,
            color="white",
        )

    # Adding total number of missing KPI variables as text in the right upper corner
    total_missing_kpi_vars = sum(len(missing_vars.split(", ")) for missing_vars in missing_kpi_vars)
    table_total_text = f"Total Missing KPI Variables: {total_missing_kpi_vars}"
    plt.text(0.65, 0.90, table_total_text, transform=plt.gca().transAxes, fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.show()
