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
import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import pandas as pd
import matplotlib.patches as patches
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from cmcrameri import cm
import math


from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv("CURRENT_VERSION")


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

logging.info("Configure paths")
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
    "pce": "total consumption",
}

logging.info("Specific age cohort")
# age_cohorts = [f"{(x-1)*5}-{(x-1)*5+4}" for x in range(5, 21)] + ["100+"]

# age_cohorts = [
#     "0-9",
#     "10-19",
#     "20-29",
#     "30-39",
#     "40-49",
#     "50-59",
#     "60-69",
#     "70-79",
#     "80-89",
#     "90+",
# ]
age_cohorts = [
    "0-19",
    "20-34",
    "34-49",
    "50-64",
    "65+",
]

cleaned_data_all_years = []
for cleaned_file in path_data_clean.glob("*_by_category*"):
    logging.info("Extract year information")
    year = cleaned_file.name.split("_")[-1].split(".")[0]

    logging.info(f"Load cleaned survey data for the year {year}")
    personal_consum = pd.read_csv(cleaned_file)

    for dependent_vari_ in consum_columns.keys():
        dependent_vari = f"{dependent_vari_}_per_capita"
        dependent_vari_name = consum_columns[dependent_vari_]

        personal_consum_no_nan = pd.DataFrame()
        for age_cohort in age_cohorts:
            try:
                age_start = int(age_cohort.split("-")[0])
            except ValueError:
                age_start = int(age_cohort.split("+")[0])
            try:
                age_end = int(age_cohort.split("-")[-1])
            except ValueError:
                age_end = 150

            logging.info("Calculate the mean consumption value")
            subset_personal_consum_ = np.array(
                personal_consum.loc[
                    (personal_consum[f"age_in_{year}"] >= age_start)
                    & (personal_consum[f"age_in_{year}"] < age_end + 1),
                    dependent_vari,
                ]
            )
            subset_personal_consum_ = subset_personal_consum_[
                ~np.isnan(subset_personal_consum_)
            ]

            logging.info("Omit outlier data")
            quantiles = np.percentile(subset_personal_consum_, [25, 75]).tolist()
            personal_consum_max = quantiles[1] + 1.5 * (quantiles[1] - quantiles[0])
            personal_consum_min = quantiles[0] + 1.5 * (quantiles[1] - quantiles[0])
            subset_personal_consum_ = subset_personal_consum_[
                (subset_personal_consum_ > personal_consum_min)
                & (subset_personal_consum_ < personal_consum_max)
            ]

            personal_consum_median = np.mean(subset_personal_consum_)
            del subset_personal_consum_

            logging.info("Calculate the mean education value")
            subset_personal_edu_ = np.array(
                personal_consum.loc[
                    (personal_consum[f"age_in_{year}"] >= age_start)
                    & (personal_consum[f"age_in_{year}"] < age_end + 1)
                    & (personal_consum[dependent_vari] > personal_consum_min)
                    & (personal_consum[dependent_vari] < personal_consum_max),
                    f"edu_in_{year}",
                ]
            )
            subset_personal_edu_ = subset_personal_edu_[~np.isnan(subset_personal_edu_)]
            personal_edu_median = np.mean(subset_personal_edu_)

            entry = {
                "age_cohort": age_cohort,
                "edu_level": personal_edu_median,
                "item": dependent_vari_name,
                "value": personal_consum_median,
                "time": year,
                "unit": "Chinese Yuan",
            }

            cleaned_data_all_years.append(entry)
            del age_cohort, subset_personal_edu_, entry

cleaned_data_all_years = pd.DataFrame(cleaned_data_all_years)

# save data
logging.info("Save cleaned data")
cleaned_data_all_years["country"] = "china"
file_name = f"{data_source}_household_data_all_years.csv"
cleaned_data_all_years.to_csv(path_data_clean / file_name, index=False)
