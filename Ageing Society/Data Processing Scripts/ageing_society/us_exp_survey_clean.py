# -*- coding: utf-8 -*-
"""
Created: Friday 14 Feb 2025
Description: Scripts to clean US expenditure survey data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.sc.at
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
data_source = "us_exp_survey"
country = "usa"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify consumption categories")
exp_categories = [
    "Food",
    "Alcoholic beverages",
    "Housing",
    "Apparel and services",
    "Transportation",
    "Healthcare",
    "Entertainment",
    "Personal care products and services",
    "Reading",
    "Education",
    "Tobacco products and smoking supplies",
    "Miscellaneous",
    "Cash contributions",
    "Personal insurance and pensions",
]

logging.info("Load the currency convert rates")
file_name_conv_rate = [file_ for file_ in path_data_raw.parent.glob("currency*")][0]
raw_conv_rate = pd.read_csv(file_name_conv_rate)
raw_conv_rate_group = raw_conv_rate.groupby("time")

logging.info("Start cleaning procedure")
cleaned_exp_full = pd.DataFrame()
for data_path in path_data_raw.glob("*.xlsx"):
    logging.info("Extract the year information")
    year = data_path.name.split("-")[-1].split(".")[0]

    logging.info(f"Load the expenditure file of year {year}")
    raw_exp = (
        pd.read_excel(
            data_path,
            skiprows=2,
            header=0,
            dtype=str,
        )
        .dropna(how="all")
        .reset_index(drop=True)
    )

    logging.info("Extract data based on unique age cohorts")
    oldest_ = 0
    raw_exp_unique = pd.DataFrame({"item": raw_exp["Item"]})
    for age_cohort in raw_exp.columns:
        age_cohort_ = age_cohort.replace("\n", " ").lower().strip()
        if "under" in age_cohort_:
            age_cohort_ = age_cohort_.replace("under ", "0-").split(" ")[0]
            raw_exp_unique[age_cohort_] = raw_exp[age_cohort]
        elif "older" in age_cohort_:
            oldest_age = int(age_cohort_.split(" ")[0])
            if oldest_age > oldest_:
                oldest_ = oldest_age
                age_cohort_oldest = age_cohort
        elif "years" in age_cohort_:
            age_cohort_ = age_cohort_.split(" ")[0]
            raw_exp_unique[age_cohort_] = raw_exp[age_cohort]
        del age_cohort_, age_cohort
    raw_exp_unique[f"{oldest_}+"] = raw_exp[age_cohort_oldest]
    raw_exp_unique = raw_exp_unique.reset_index(drop=True)
    del raw_exp

    logging.info("Find the index of expenditure categories and other key information")
    item_list = list(raw_exp_unique["item"])
    item_maping = {
        "household_size": item_list.index("People"),
        "total": item_list.index("Average annual expenditures") + 1,
        # "total_mean": item_list.index("Average annual expenditures") + 1,
        # "total_se": item_list.index("Average annual expenditures") + 2, # se is standard error
    }  # to map the item name and item index of key information and expenditure categories
    for item_ in exp_categories:
        # item_maping[f"{item_.lower()}_mean"] = item_list.index(item_) + 1
        # item_maping[f"{item_.lower()}_se"] = item_list.index(item_) + 3
        item_maping[item_.lower()] = item_list.index(item_) + 1
    raw_exp_item = (
        raw_exp_unique.iloc[list(item_maping.values()), :]
        .reset_index(drop=True)
        .replace("c/", np.nan)
    )
    raw_exp_item["item"] = list(item_maping.keys())

    logging.info("Calculate the per-capita expenditure")
    cleaned_exp_per_capita = pd.DataFrame()
    for pos, column in enumerate(raw_exp_item.columns):
        if pos >= 1:
            raw_exp_item[column] = raw_exp_item[column].astype(float)

            exp_age_cohort = raw_exp_item.iloc[1:, [0, pos]]
            househouse_size = raw_exp_item.iloc[0, pos]

            logging.info("Find the currency convert rate")
            raw_conv_rate_ = raw_conv_rate_group.get_group(int(year))[
                f"{country}, convert rate local currency to 2005 usd"
            ].values[0]

            exp_age_cohort["value"] = (
                exp_age_cohort[column] / househouse_size * raw_conv_rate_
            )
            exp_age_cohort["age_cohort"] = column
            exp_age_cohort["household_size"] = househouse_size

            cleaned_exp_per_capita = pd.concat(
                [cleaned_exp_per_capita, exp_age_cohort.drop(columns=[column])]
            )
            del exp_age_cohort, househouse_size, pos, raw_conv_rate_
    cleaned_exp_per_capita["time"] = year
    cleaned_exp_per_capita["unit"] = "in 2005 usd"

    logging.info("Concat cleaned data from different years")
    cleaned_exp_full = pd.concat([cleaned_exp_full, cleaned_exp_per_capita])
    del cleaned_exp_per_capita


# save data
logging.info("Save cleaned data")
cleaned_exp_full["country"] = "united states of america"
file_name = f"{data_source}_household_data_all_in_2005_usd.csv"
cleaned_exp_full.to_csv(path_data_clean / file_name, index=False)
