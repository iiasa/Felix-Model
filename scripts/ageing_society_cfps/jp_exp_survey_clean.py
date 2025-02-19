# -*- coding: utf-8 -*-
"""
Created: Monday 17 Feb 2025
Description: Scripts to clean JP expenditure survey data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.sc.at
"""

import datetime
import logging
from pathlib import Path

import pandas as pd
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
data_source = "jp_exp_survey"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify consumption categories")
exp_categories = [
    "Consumption expenditures",
    "Food",
    "Housing",
    "Fuel, light & water charges",
    "Furniture & household utensils",
    "Clothing & footwear",
    "Medical care",
    "Transportation & communication",
    "Education",
    "Culture & recreation",
    "Other consumption expenditures",
]

logging.info("Start cleaning procedure")
cleaned_exp_full = pd.DataFrame()
for data_path in path_data_raw.glob("*.xls*"):
    logging.info("Extract the year information")
    year = data_path.name.split("_")[-1].split(".")[0]

    logging.info(f"Load the expenditure file of year {year}")
    if int(year) < 2018:
        use_cols = "H:AF"
    else:
        use_cols = "H:AH"
    raw_exp_all = pd.read_excel(
        data_path,
        sheet_name=None,
        skiprows=14,
        usecols=use_cols,
        header=0,
        dtype=str,
    )
    sheet_names = list(raw_exp_all.keys())
    raw_exp = raw_exp_all[sheet_names[0]].dropna(how="all").reset_index(drop=True)

    logging.info("Extract data based on unique age cohorts")
    oldest_ = 0
    raw_exp_unique = pd.DataFrame({"item": raw_exp.iloc[:, -1]})
    if int(year) > 2006:
        index_household_size = list(raw_exp_unique["item"]).index(
            "Num. of persons per household (persons)"
        )
        index_age = list(raw_exp_unique["item"]).index(
            "Age of household heads (years old)"
        )
    else:
        index_household_size = list(raw_exp_unique["item"]).index(
            "Persons per household (persons)"
        )
        index_age = list(raw_exp_unique["item"]).index("Age of head (years old)")

    for pos, year_true in enumerate(raw_exp.columns.str.contains("years")):
        if year_true:
            person_age_ = float(raw_exp.iloc[index_age, pos])
            if person_age_ < 30:
                age_cohort_ = "0-29"
            elif person_age_ < 40:
                age_cohort_ = "30-39"
            elif person_age_ < 50:
                age_cohort_ = "40-49"
            elif person_age_ < 60:
                age_cohort_ = "50-59"
            elif person_age_ < 70:
                age_cohort_ = "60-69"
            else:
                age_cohort_ = "70+"

            if age_cohort_ not in raw_exp_unique.columns:
                raw_exp_unique[age_cohort_] = raw_exp.iloc[:, pos]
                del age_cohort_
    raw_exp_unique = raw_exp_unique.reset_index(drop=True)
    del raw_exp

    logging.info("Find the index of expenditure categories and other key information")
    item_list = list(raw_exp_unique["item"])

    # to map the item name and item index of key information and expenditure categories
    item_maping = {"household_size": index_household_size}
    for pos, item_ in enumerate(exp_categories):
        if pos == 0:
            if int(year) > 2006:
                item_maping[item_.lower()] = item_list.index(item_)
            else:
                logging.info("Adjust the name of total expenditure")
                item_maping[item_.lower()] = item_list.index("Living expenditure")
        else:
            if "water charges" in item_:
                if int(year) > 2008:
                    item_maping[item_.lower()] = item_list.index(item_)
                else:
                    logging.info("Adjust the name of fuel, light and water charges")
                    item_maping[item_.lower()] = item_list.index(
                        "Fuel,light & water charges"
                    )
            else:
                if "& footwear" in item_:
                    if int(year) > 2006:
                        item_maping[item_.lower()] = item_list.index(item_)
                    else:
                        logging.info("Adjust the name of clothing and footwear")
                        item_maping[item_.lower()] = item_list.index(
                            "Clothes & footwear"
                        )
                else:
                    if "recreation" in item_:
                        if int(year) > 2006:
                            item_maping[item_.lower()] = item_list.index(item_)
                        else:
                            logging.info("Adjust the name of reading and recreation")
                            item_maping[item_.lower()] = item_list.index(
                                "Reading & recreation"
                            )
                    else:
                        if "Other" in item_:
                            if int(year) > 2006:
                                item_maping[item_.lower()] = item_list.index(item_)
                            else:
                                logging.info("Adjust the name of other expenditure")
                                item_maping[item_.lower()] = item_list.index(
                                    "Other living expenditure"
                                )
                        else:
                            item_maping[item_.lower()] = item_list.index(item_)

    raw_exp_item = raw_exp_unique.iloc[list(item_maping.values()), :].reset_index(
        drop=True
    )
    raw_exp_item["item"] = list(item_maping.keys())

    logging.info("Calculate the per-capita expenditure")
    cleaned_exp_per_capita = pd.DataFrame()
    for pos, column in enumerate(raw_exp_item.columns):
        if pos >= 1:
            raw_exp_item[column] = raw_exp_item[column].astype(float)

            exp_age_cohort = raw_exp_item.iloc[1:, [0, pos]]
            househouse_size = raw_exp_item.iloc[0, pos]

            exp_age_cohort["value"] = (
                exp_age_cohort[column] / househouse_size * 12
            )  # convert to annual expenditure
            exp_age_cohort["age_cohort"] = column
            exp_age_cohort["household_size"] = househouse_size

            cleaned_exp_per_capita = pd.concat(
                [cleaned_exp_per_capita, exp_age_cohort.drop(columns=[column])]
            )
            del exp_age_cohort, househouse_size, pos
    cleaned_exp_per_capita["time"] = year
    cleaned_exp_per_capita["unit"] = "current prices, japanese yen"

    logging.info("Concat cleaned data from different years")
    cleaned_exp_full = pd.concat([cleaned_exp_full, cleaned_exp_per_capita])
    del cleaned_exp_per_capita


# save data
logging.info("Save cleaned data")
file_name = f"{data_source}_household_data_all.csv"
cleaned_exp_full.to_csv(path_data_clean / file_name, index=False)
