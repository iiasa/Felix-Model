"""
Created: Tuesday 11 Feb 2025
Description: Scripts to match personal age and consumption data from eurostat
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import datetime
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
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
data_source = "eurostat"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify file name of input data")
file_name_exp = "hbs_exp_t135__custom_15321137_spreadsheet.xlsx"
file_name_exp_str = "hbs_str_t225__custom_15321283_spreadsheet.xlsx"

logging.info("Read the expenditure data")
raw_exp = pd.read_excel(
    path_data_raw / file_name_exp,
    sheet_name="Sheet 1",
    skiprows=[i for i in range(8)] + [9],
    header=0,
    dtype=str,
    skipfooter=4,
)
raw_exp.columns = [column_.replace("*", "") for column_ in raw_exp.columns]
raw_exp = raw_exp.rename(
    columns={
        "GEO (Labels)": "age_cohort",
    }
).set_index("age_cohort")

logging.info("Read the expenditure structure data")
raw_exp_str = pd.read_excel(
    path_data_raw / file_name_exp_str,
    sheet_name="Sheet 1",
    skiprows=[i for i in range(8)] + [9],
    header=0,
    dtype=str,
    skipfooter=3,
)
raw_exp_str.columns = [column_.replace("*", "") for column_ in raw_exp_str.columns]
raw_exp_str = raw_exp_str.rename(
    columns={
        "GEO (Labels)": "age_cohort",
        "GEO (Labels).1": "exp_category",
    }
)
logging.info("Replace ':' with NaN")
raw_exp_str.replace(":", np.nan, inplace=True)

logging.info("Configure data years")
year = 2015

logging.info("Specify age cohorts")
age_cohorts = list(np.unique(raw_exp_str["age_cohort"]))
age_cohorts.remove("Unknown")
logging.info("Specify consumption category")
exp_categories = list(np.unique(raw_exp_str["exp_category"]))
logging.info("Specify countries")
eu_countries = list(raw_exp_str.columns)[2:]

logging.info("Convert data type into numeric")
for country in eu_countries:
    raw_exp[country] = pd.to_numeric(
        raw_exp[country],
        errors="coerce",
    )
    raw_exp_str[country] = pd.to_numeric(
        raw_exp_str[country],
        errors="coerce",
    )

logging.info("Group by age and category")
raw_exp_str_groups = raw_exp_str.groupby("age_cohort")
clean_exp = pd.DataFrame()
for age_cohort in age_cohorts:
    raw_exp_str_age = raw_exp_str_groups.get_group(age_cohort)
    for country in eu_countries:
        raw_exp_str_country = raw_exp_str_age[["age_cohort", "exp_category", country]]
        raw_exp_country = raw_exp.loc[age_cohort, country]
        raw_exp_str_country["value"] = (
            raw_exp_str_country[country] / (raw_exp_str_country[country]).sum()
        ) * raw_exp_country
        raw_exp_str_country["country"] = country
        raw_exp_str_country = raw_exp_str_country.drop(columns=[country])
        clean_exp = pd.concat([clean_exp, raw_exp_str_country], ignore_index=True)
        del country, raw_exp_str_country

# save data
logging.info("Save cleaned data")
file_name = f"{data_source}_personal_all_{year}.csv"
clean_exp.to_csv(path_data_clean / file_name, index=False)
