# -*- coding: utf-8 -*-
"""
Created: Fri 21 Feb 2025
Description: Scripts to merge CFPS data from multiple years
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""
import datetime
import logging
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_AGEING_SOCIETY")


timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Specify the log message format
    datefmt="%Y-%m-%d %H:%M:%S",  # Specify the date format
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("app.log"),  # Log to a file
    ],
)

logging.info("Configure module")
current_module = "ageing_society"

logging.info("Configure data source")
data_source = "cfps"
country = "china"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

logging.info("Specify categories of consumption")
consum_columns = {
    "daily": "Household equipment and daily necessities expenditure",
    "dress": "Clothing and shoes expenditure",
    "eec": "Education and entertainment expenditure",
    "food": "Food expenditure",
    "house": "Residential expenditure",
    "med": "Health care expenditure",
    "trco": "Transportation and communication expenditure",
    "other": "Other expenditure",
    # "pce": "total consumption",
}

logging.info("Specific age cohort")
# for the final cohort classification
age_cohorts = [
    "0-14",
    "15-24",
    "25-44",
    "45-64",
    "65+",
]

logging.info("Load the currency convert rates")
file_name_conv_rate = [file_ for file_ in path_data_raw.parent.glob("currency*")][0]
raw_conv_rate = pd.read_csv(file_name_conv_rate)
raw_conv_rate_group = raw_conv_rate.groupby("time")


def omit_outlier(data_series: np.array, item: str, age_cohort: str):
    """
    To omit outliers of a data series

    Input parameters:
    data_series: np.array
        A data series that needs to omit outliers
    item: str
        The name of expenditure item
    age_cohort: str
        The age cohort

    Return:
        data_series_without_outliers
    """
    logging.info("Omit nan values")
    data_series_ = data_series[~np.isnan(data_series)]

    logging.info("Calculate the quantiles")
    if item == "daily":
        if age_cohort in ["0-14"]:
            quantiles = np.percentile(data_series_, [70, 95]).tolist()
        elif age_cohort in ["15-24"]:
            quantiles = np.percentile(data_series_, [50, 95]).tolist()
        elif age_cohort in ["25-44", "45-64"]:
            quantiles = np.percentile(data_series_, [35, 95]).tolist()
        elif age_cohort == "65+":
            quantiles = np.percentile(data_series_, [60, 95]).tolist()
    elif item == "dress":
        if age_cohort in ["0-14", "15-24"]:
            quantiles = np.percentile(data_series_, [90, 95]).tolist()
        elif age_cohort in ["25-44", "45-64"]:
            quantiles = np.percentile(data_series_, [65, 95]).tolist()
        elif age_cohort == "65+":
            quantiles = np.percentile(data_series_, [75, 95]).tolist()
    elif item == "eec":
        if age_cohort in ["0-14"]:
            quantiles = np.percentile(data_series_, [90, 95]).tolist()
        elif age_cohort in ["15-24", "25-44", "45-64"]:
            quantiles = np.percentile(data_series_, [60, 95]).tolist()
        elif age_cohort == "65+":
            quantiles = np.percentile(data_series_, [75, 95]).tolist()
    elif item == "food":
        if age_cohort in ["0-14"]:
            quantiles = np.percentile(data_series_, [80, 95]).tolist()
        elif age_cohort in ["15-24"]:
            quantiles = np.percentile(data_series_, [60, 95]).tolist()
        elif age_cohort in ["25-44", "45-64", "65+"]:
            quantiles = np.percentile(data_series_, [25, 95]).tolist()
    elif item == "house":
        if age_cohort in ["0-14", "15-24"]:
            quantiles = np.percentile(data_series_, [80, 95]).tolist()
        elif age_cohort in ["25-44", "45-64"]:
            quantiles = np.percentile(data_series_, [75, 95]).tolist()
        elif age_cohort in ["65+"]:
            quantiles = np.percentile(data_series_, [80, 95]).tolist()
    elif item == "med":
        if age_cohort in ["0-14"]:
            quantiles = np.percentile(data_series_, [80, 95]).tolist()
        elif age_cohort in ["15-24"]:
            quantiles = np.percentile(data_series_, [60, 95]).tolist()
        elif age_cohort in ["25-44", "45-64"]:
            quantiles = np.percentile(data_series_, [45, 95]).tolist()
        elif age_cohort in ["65+"]:
            quantiles = np.percentile(data_series_, [50, 95]).tolist()
    elif item == "trco":
        if age_cohort in ["0-14", "15-24"]:
            quantiles = np.percentile(data_series_, [90, 95]).tolist()
        elif age_cohort in ["25-44", "45-64", "65+"]:
            quantiles = np.percentile(data_series_, [75, 95]).tolist()
    elif item == "other":
        quantiles = np.percentile(data_series_, [90, 95]).tolist()
    data_series_without_outliers = data_series_[
        (data_series_ > quantiles[0]) & (data_series_ <= quantiles[1])
    ]

    if len(data_series_without_outliers) > 0:
        return data_series_without_outliers, quantiles[1], quantiles[0]
    else:
        logging.warning("No valid values left")
        return np.array()


cleaned_data_all_years = []
for cleaned_file in path_data_clean.glob("*_by_category*"):
    logging.info("Extract year information")
    year = cleaned_file.name.split("_")[-1].split(".")[0]

    logging.info(f"Load cleaned survey data for the year {year}")
    personal_consum = pd.read_csv(cleaned_file)

    for age_cohort in age_cohorts:
        try:
            age_start = int(age_cohort.split("-")[0])
        except ValueError:
            age_start = int(age_cohort.split("+")[0])
        try:
            age_end = int(age_cohort.split("-")[-1])
        except ValueError:
            age_end = 150

        total_exp = 0
        total_edu = 0
        for dependent_vari_ in consum_columns.keys():
            dependent_vari = f"{dependent_vari_}_per_capita"
            dependent_vari_name = consum_columns[dependent_vari_]

            logging.info("Calculate the mean consumption value")
            subset_personal_consum_ = np.array(
                personal_consum.loc[
                    (personal_consum[f"age_in_{year}"] >= age_start)
                    & (personal_consum[f"age_in_{year}"] < age_end + 1),
                    dependent_vari,
                ]
            )
            subset_personal_consum_, personal_consum_max, personal_consum_min = (
                omit_outlier(
                    data_series=subset_personal_consum_,
                    item=dependent_vari_,
                    age_cohort=age_cohort,
                )
            )

            personal_consum_median = np.median(subset_personal_consum_)
            del subset_personal_consum_

            logging.info("Calculate the mean education value")
            subset_personal_edu_ = np.array(
                personal_consum.loc[
                    (personal_consum[f"age_in_{year}"] >= age_start)
                    & (personal_consum[f"age_in_{year}"] < age_end + 1)
                    & (personal_consum[dependent_vari] > personal_consum_min)
                    & (personal_consum[dependent_vari] <= personal_consum_max),
                    f"edu_in_{year}",
                ]
            )
            subset_personal_edu_ = subset_personal_edu_[~np.isnan(subset_personal_edu_)]
            personal_edu_median = np.median(subset_personal_edu_)

            logging.info("Find the currency convert rate")
            raw_conv_rate_ = raw_conv_rate_group.get_group(int(year))[
                f"{country}, convert rate local currency to 2005 usd"
            ].values[0]

            entry = {
                "age_cohort": age_cohort,
                "edu_level": personal_edu_median,
                "item": dependent_vari_name,
                "value": personal_consum_median * raw_conv_rate_,
                "time": year,
                "unit": "2005 usd",
            }

            cleaned_data_all_years.append(entry)
            total_exp += personal_consum_median * raw_conv_rate_
            total_edu += personal_edu_median
            del subset_personal_edu_, entry, raw_conv_rate_, dependent_vari_name

        # total expenditure
        entry = {
            "age_cohort": age_cohort,
            "edu_level": total_edu / 8,
            "item": "total consumption",
            "value": total_exp,
            "time": year,
            "unit": "2005 usd",
        }
        cleaned_data_all_years.append(entry)
        del total_edu, total_exp, age_cohort, entry

cleaned_data_all_years = pd.DataFrame(cleaned_data_all_years)

# save data
logging.info("Save cleaned data")
cleaned_data_all_years["country"] = country
file_name = f"{data_source}_household_data_all_years_in_2005_usd.csv"
cleaned_data_all_years.to_csv(path_data_clean / file_name, index=False)
