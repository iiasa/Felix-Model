"""
Created: Wed 5 Mar 2025
Description: Scripts to convert national expenditure data into five FeliX regions
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

logging.info("Configure paths")
path_data_clean = data_home / "clean_data" / current_module / current_version

logging.info("Load cleaned expenditure data in felix classifications")
cleaned_exp_file_name = "personal_data_in_felix_classification.csv"
cleaned_exp = pd.read_csv(path_data_clean / cleaned_exp_file_name)
cleaned_exp_groups = cleaned_exp.groupby(["felix_age_cohort", "felix_item", "time"])

logging.info("Load population data")
path_population_clean = (
    data_home / "clean_data" / "felix_regionalization" / "v.11.2023" / "population"
)
population_file_name = "population_by_location_and_age_cohort_for_ageing_society.csv"
cleaned_population = pd.read_csv(path_population_clean / population_file_name)
cleaned_population_groups = cleaned_population.groupby(
    ["location", "felix_age_cohort", "year"]
)
cleaned_population_felix = []
for location in np.unique(cleaned_population["location"]):
    for felix_age_cohort in np.unique(cleaned_exp["felix_age_cohort"]):
        for year in np.unique(cleaned_population["year"]):
            cleaned_population_region_year = cleaned_population_groups.get_group(
                (location, felix_age_cohort, year)
            )
            entry = {
                "country": location,
                "felix_region": list(
                    np.unique(cleaned_population_region_year["felix_region"])
                )[0],
                "felix_age_cohort": felix_age_cohort,
                "time": year,
                "value_population": cleaned_population_region_year["value"].sum(),
                "unit": "person",
            }
            cleaned_population_felix.append(entry)
            del entry, cleaned_population_region_year
cleaned_population_felix = pd.DataFrame(cleaned_population_felix)
cleaned_population_groups = cleaned_population_felix.groupby(
    ["felix_age_cohort", "time"]
)
del (
    population_file_name,
    path_population_clean,
    cleaned_population,
)

logging.info("Configure basic information of the expenditure dataset")
felix_age_cohorts = list(np.unique(cleaned_exp["felix_age_cohort"]))
felix_items = list(np.unique(cleaned_exp["felix_item"]))
years = list(np.unique(cleaned_exp["time"]))
unit = list(np.unique(cleaned_exp["unit"]))[0]
del cleaned_exp, cleaned_population_felix

logging.info(
    "Start to aggregate national data into regional one, and restructure the data"
)
exp_data_felix_region = []
for felix_age_cohort in felix_age_cohorts:
    for felix_item in felix_items:
        for year in years:
            try:
                cleaned_exp_region_year = cleaned_exp_groups.get_group(
                    (felix_age_cohort, felix_item, year)
                )
            except KeyError:
                continue

            if len(np.unique(cleaned_exp_region_year["country"])) > 1:
                cleaned_population_region_year = cleaned_population_groups.get_group(
                    (felix_age_cohort, year)
                )

                cleaned_exp_pop_merge = pd.merge(
                    cleaned_exp_region_year,
                    cleaned_population_region_year[
                        ["country", "felix_region", "value_population"]
                    ],
                    on="country",
                    how="left",
                )

                logging.info("Calculate global average")
                entry = {
                    "felix_region": "World",
                    "felix_item": felix_item,
                    "felix_age_cohort": felix_age_cohort,
                    "time": year,
                }
                for variable in ["household_size", "edu_level", "value"]:
                    entry[variable] = sum(
                        np.array(cleaned_exp_pop_merge[variable])
                        * np.array(cleaned_exp_pop_merge["value_population"])
                    ) / np.sum(cleaned_exp_pop_merge["value_population"])

                entry["unit"] = "2005 usd"
                exp_data_felix_region.append(entry)
                del entry

                logging.info("Calculate regional average")
                cleaned_exp_pop_merge_groups = cleaned_exp_pop_merge.groupby(
                    "felix_region"
                )
                for felix_region in np.unique(cleaned_exp_pop_merge["felix_region"]):
                    cleaned_exp_pop_region = cleaned_exp_pop_merge_groups.get_group(
                        felix_region
                    )

                    entry = {
                        "felix_region": felix_region,
                        "felix_item": felix_item,
                        "felix_age_cohort": felix_age_cohort,
                        "time": year,
                    }
                    for variable in ["household_size", "edu_level", "value"]:
                        entry[variable] = sum(
                            np.array(cleaned_exp_pop_region[variable])
                            * np.array(cleaned_exp_pop_region["value_population"])
                        ) / np.sum(cleaned_exp_pop_region["value_population"])

                    entry["unit"] = "2005 usd"
                    exp_data_felix_region.append(entry)
                    del entry

            else:
                logging.info("Only one country with data, we don't use it")
                continue

logging.info("Restructure the data")
exp_data_felix_region = pd.DataFrame(exp_data_felix_region)
exp_data_felix_region_groups = exp_data_felix_region.groupby(
    ["felix_region", "felix_item", "felix_age_cohort", "time"]
)
exp_data_time_series = []
for felix_item in np.unique(exp_data_felix_region["felix_item"]):
    for felix_region in np.unique(exp_data_felix_region["felix_region"]):
        for felix_age_cohort in np.unique(exp_data_felix_region["felix_age_cohort"]):
            entry = {"parameter": f"{felix_item}[{felix_region},{felix_age_cohort}]"}
            entry_household = {
                "parameter": f"Household Size[{felix_region},{felix_age_cohort}]"
            }
            entry_edu_level = {
                "parameter": f"Educational Level[{felix_region},{felix_age_cohort}]"
            }
            for year in range(1900, 2101):
                if year in np.unique(exp_data_felix_region["time"]):
                    try:
                        exp_data_ = exp_data_felix_region_groups.get_group(
                            (felix_region, felix_item, felix_age_cohort, year)
                        )
                        value_ = exp_data_["value"].values[0]
                        value_household = exp_data_["household_size"].values[0]
                        value_edu_level = exp_data_["edu_level"].values[0]
                    except KeyError:
                        value_ = None
                        value_household = None
                        value_edu_level = None
                else:
                    value_ = None
                    value_household = None
                    value_edu_level = None

                entry[year] = value_
                entry_household[year] = value_household
                entry_edu_level[year] = value_edu_level
                del year, value_, value_household, value_edu_level

            exp_data_time_series.append(entry)
            if felix_item == list(np.unique(exp_data_felix_region["felix_item"]))[-1]:
                exp_data_time_series.append(entry_household)
                exp_data_time_series.append(entry_edu_level)
                del entry_household, entry_edu_level
            del entry

exp_data_time_series = pd.DataFrame(exp_data_time_series).replace(0, np.nan)

# save data
logging.info("Save cleaned data")
file_name = f"personal_data_in_felix_classification_time_series.csv"
exp_data_time_series.to_csv(path_data_clean / file_name, index=False)
