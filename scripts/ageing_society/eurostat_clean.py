"""
Created: Tuesday 11 Feb 2025
Description: Scripts to match personal age and consumption data from eurostat
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
data_source = "eurostat"
country = "euro"


logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify file name of input data")
file_name_exp = [file_ for file_ in path_data_raw.glob("hbs_exp_t135*")][0]
file_name_exp_str = [file_ for file_ in path_data_raw.glob("hbs_str_t225*")][0]
file_name_conv_rate = [file_ for file_ in path_data_raw.parent.glob("currency*")][0]

logging.info("Read the expenditure data")
raw_exp = pd.read_excel(
    file_name_exp,
    sheet_name="Sheet 1",
    skiprows=[i for i in range(7)] + [9],
    header=[0, 1],
    dtype=str,
    skipfooter=4,
)
raw_exp.columns = [
    f"{column_[0]}-{column_[1].replace('*', '')}" for column_ in raw_exp.columns
]

raw_exp = raw_exp.rename(
    columns={
        "TIME-GEO (Labels)": "age_cohort",
    }
).set_index("age_cohort")

logging.info("Read the currency convert rates")
raw_conv_rate = pd.read_csv(file_name_conv_rate)
raw_conv_rate_group = raw_conv_rate.groupby("time")

logging.info("Convert expenditure data to 2005 usd")
for column_ in raw_exp.columns:
    year = int(column_.split("-")[0])
    raw_exp_country_ = raw_exp[column_]
    raw_conv_rate_country_ = raw_conv_rate_group.get_group(year)[
        f"{country}, convert rate local currency to 2005 usd"
    ].values[0]
    try:
        raw_exp_country_ = np.array(raw_exp_country_, dtype="float")
    except ValueError:
        raw_exp[column_] = ""
        continue
    raw_exp[column_] = (
        np.array(raw_exp_country_, dtype="float") * raw_conv_rate_country_
    )
    del year, raw_exp_country_, raw_conv_rate_country_

logging.info("Read the expenditure structure data")
raw_exp_str = pd.read_excel(
    path_data_raw / file_name_exp_str,
    sheet_name="Sheet 1",
    skiprows=[i for i in range(7)] + [9],
    header=[0, 1],
    dtype=str,
    skipfooter=3,
)

raw_exp_str.columns = [
    f"{column_[0]}-{column_[1].replace('*', '')}" for column_ in raw_exp_str.columns
]

raw_exp_str = raw_exp_str.rename(
    columns={
        "TIME-GEO (Labels)": "age_cohort",
        "TIME-GEO (Labels).1": "exp_category",
    }
)
logging.info("Replace ':' with NaN")
raw_exp_str.replace(":", np.nan, inplace=True)

logging.info("Specify age cohorts")
age_cohorts = list(np.unique(raw_exp_str["age_cohort"]))
age_cohorts.remove("Unknown")
logging.info("Specify consumption category")
exp_categories = list(np.unique(raw_exp_str["exp_category"]))

logging.info("Configure data years")
years = list(set([column_.split("-")[0] for column_ in list(raw_exp_str.columns)[2:]]))
logging.info("Specify countries")
eu_countries = list(
    set([column_.split("-")[-1] for column_ in list(raw_exp_str.columns)[2:]])
)

logging.info("Convert data type into numeric")
for year in years:
    for country in eu_countries:
        column_name = f"{year}-{country}"
        raw_exp[column_name] = pd.to_numeric(
            raw_exp[column_name],
            errors="coerce",
        )
        raw_exp_str[column_name] = pd.to_numeric(
            raw_exp_str[column_name],
            errors="coerce",
        )

logging.info("Group by age and category")
raw_exp_str_groups = raw_exp_str.groupby("age_cohort")
clean_exp = pd.DataFrame()
for age_cohort in age_cohorts:
    age_cohort_ = (
        age_cohort.replace(" years", "")
        .replace(" to ", "-")
        .replace("Less than ", "0-")
        .replace("From ", "")
        .replace(" or over", "+")
    )

    raw_exp_str_age = raw_exp_str_groups.get_group(age_cohort)

    for year in years:
        for country in eu_countries:
            column_name = f"{year}-{country}"
            raw_exp_str_country = raw_exp_str_age[
                ["age_cohort", "exp_category", column_name]
            ]

            raw_exp_country = raw_exp.loc[age_cohort, column_name]

            raw_exp_str_country["value"] = (
                raw_exp_str_country[column_name]
                / (raw_exp_str_country[column_name]).sum()
            ) * raw_exp_country

            raw_exp_str_country["age_cohort"] = age_cohort_
            raw_exp_str_country["time"] = year
            raw_exp_str_country["country"] = country
            raw_exp_str_country = raw_exp_str_country.drop(
                columns=[column_name]
            ).reset_index(drop=True)
            raw_exp_str_country.loc[len(raw_exp_str_country)] = [
                age_cohort_,
                "Total",
                raw_exp_country,
                year,
                country,
            ]

            clean_exp = pd.concat([clean_exp, raw_exp_str_country], ignore_index=True)
            del country, raw_exp_str_country, raw_exp_country

clean_exp = clean_exp.rename(columns={"exp_category": "item"})
clean_exp["unit"] = "2005 usd"
# save data
logging.info("Save cleaned data")
file_name = f"{data_source}_personal_data_all_years_in_2005_usd.csv"
clean_exp.to_csv(path_data_clean / file_name, index=False)
