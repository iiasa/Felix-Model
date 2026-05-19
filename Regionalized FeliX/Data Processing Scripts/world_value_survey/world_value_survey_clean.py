# -*- coding: utf-8 -*-
"""
Created: Tuesday 10 Feb 2026
Description: Scripts to clean world value survey data
Scope: Ageing society project, module age_expenditure
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.sc.at
"""

import datetime
import logging
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_AGEING_IMPACT")

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
current_module = "age_expenditure"

logging.info("Configure data source")
data_source = "world_value_survey"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify variable codes")
variable_mapping = {
    "S003": "country_code",  # ISO 3166-1 numeric country code
    "COUNTRY_ALPHA": "country_alpha",  # the three-letter abbreviation of countries
    "S007": "person_id",  # Unified respondent number
    "X002": "birth_year",  # Year of birth
    "X003": "age",  # Age
    "X025R": "edu_level",  # Education level (recoded)
    "S020": "survey_year",  # Year survey
    "B008": "environ_vs_econ",  # Protecting environment vs. Economic growth
    "B001": "income_for_environ",  # Would give part of my income for environment
    "B002": "taxes_for_environ",  # Increase in taxes if extra money used to prevent environmental pollution
}

logging.info("Specify data file name")
try:
    raw_wvs_file = [
        file_ for file_ in path_data_raw.glob("test_WVS_Time_Series_*.xlsx")
    ][0]
except IndexError:
    raise IndexError("No data available in the folder")

try:
    raw_wvs_country_file = [
        file_ for file_ in path_data_raw.glob("*CountrySpecificCodes*")
    ][0]
except IndexError:
    raise IndexError("No data available in the folder")

logging.info("Load raw data (omitting invalid data rows) and country codes")
raw_wvs_data = pd.read_excel(
    raw_wvs_file,
    sheet_name=0,
    header=0,
    dtype=str,
).rename(columns=variable_mapping)
variable_set_as_int = [
    "birth_year",
    "edu_level",
    "survey_year",
    "environ_vs_econ",
    "income_for_environ",
    "taxes_for_environ",
]
raw_wvs_data[variable_set_as_int] = raw_wvs_data[variable_set_as_int].astype(int)
raw_wvs_data = raw_wvs_data[
    (raw_wvs_data["birth_year"] > 0) & (raw_wvs_data["edu_level"] > 0)
]
raw_wvs_data["age"] = raw_wvs_data["survey_year"] - raw_wvs_data["birth_year"]

logging.info(
    "Only consider 'agree' and 'disagree' for 'taxes' and 'income' for environment"
)
logging.info("Merge 1 and 2 as 1 for agree; merge 3 and 4 for 2 as disagree")
for merge_variable_ in ["income_for_environ", "taxes_for_environ"]:
    raw_wvs_data.loc[raw_wvs_data[merge_variable_] == 2, merge_variable_] = 1
    raw_wvs_data.loc[
        (raw_wvs_data[merge_variable_] == 3) | (raw_wvs_data[merge_variable_] == 4),
        merge_variable_,
    ] = 2
    del merge_variable_
del variable_set_as_int


logging.info("Country code and country name to be matched")
raw_wvs_country_code = pd.read_excel(
    raw_wvs_country_file,
    sheet_name=0,
    header=None,
    dtype=str,
)
raw_wvs_country_code.columns = ["country_code", "country_name"]
raw_wvs_data = pd.merge(raw_wvs_data, raw_wvs_country_code, on="country_code")
del raw_wvs_country_code

logging.info("Specify age cohorts")
age_bins = [0, 24, 44, 64, 200]
age_cohorts = ["0-24", "25-44", "45-64", "65+"]
raw_wvs_data["age_cohort"] = pd.cut(
    raw_wvs_data["age"],
    bins=age_bins,
    labels=age_cohorts,
    right=True,
    include_lowest=True,
)
del age_bins

logging.info("Convert educational levels into educational years")
edu_year_mapping = {1: 6, 2: 12, 3: 16}
raw_wvs_data["mean_year_schooling"] = raw_wvs_data["edu_level"].map(edu_year_mapping)
del edu_year_mapping

logging.info("Specify country names")
countries = list(np.unique(raw_wvs_data["country_name"]))
years = list(np.unique(raw_wvs_data["survey_year"]))

logging.info("Start cleaning procedure")
raw_wvs_data_groups = raw_wvs_data.groupby(
    ["country_name", "survey_year", "age_cohort"]
)

variables_to_clean = ["environ_vs_econ", "income_for_environ", "taxes_for_environ"]
# environ_vs_econ: 1 = environ > econ; 2 = environ < econ; 3 = other answers
# income / taxes for environ: 1 = agree; 2 = disagree
cleaned_wvs_data = []
for country_ in countries:
    for year_ in years:
        for age_cohort_ in age_cohorts:
            try:
                raw_wvs_country_year_age = raw_wvs_data_groups.get_group(
                    (country_, year_, age_cohort_)
                )
            except KeyError:
                continue

            raw_wvs_country_year_age = raw_wvs_country_year_age[
                raw_wvs_country_year_age[variables_to_clean]
                .isin({1, 2, 3, -4})
                .any(axis=1)
            ]

            # avg = df[cols].replace(-4, np.nan).mean(axis=1)

            age_average_ = np.mean(raw_wvs_country_year_age["age"])
            mean_year_schooling_ = np.mean(
                raw_wvs_country_year_age["mean_year_schooling"]
            )
            if country_ == "Turkey":
                entry = {
                    "country": "Türkiye",
                    "time": year_,
                    "felix_age_cohort": age_cohort_,
                    "age_average": age_average_,
                    "mean_year_schooling": mean_year_schooling_,
                }
            elif country_ == "United States":
                entry = {
                    "country": "united states of america",
                    "time": year_,
                    "felix_age_cohort": age_cohort_,
                    "age_average": age_average_,
                    "mean_year_schooling": mean_year_schooling_,
                }
            else:
                entry = {
                    "country": country_.lower(),
                    "time": year_,
                    "felix_age_cohort": age_cohort_,
                    "age_average": age_average_,
                    "mean_year_schooling": mean_year_schooling_,
                }

            for variable_to_clean_ in variables_to_clean:
                variable_to_clean_value_ = (
                    raw_wvs_country_year_age[
                        raw_wvs_country_year_age[variable_to_clean_].isin([1, 2, 3])
                    ][variable_to_clean_]
                    == 1
                ).mean()

                entry[variable_to_clean_] = variable_to_clean_value_
                del variable_to_clean_, variable_to_clean_value_

            cleaned_wvs_data.append(entry)
            del (entry, mean_year_schooling_, age_average_, raw_wvs_country_year_age)
cleaned_wvs_data = pd.DataFrame(cleaned_wvs_data)


# save data
logging.info("Save cleaned data")
file_name = f"{data_source}_data_time_series.csv"
cleaned_wvs_data.to_csv(path_data_clean / file_name, index=False)
