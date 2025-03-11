# -*- coding: utf-8 -*-
"""
Created: Tuesday 18 Feb 2025
Description: Scripts to clean AU expenditure survey data
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
data_source = "au_exp_survey"
country = "australia"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

logging.info("Load the currency convert rates")
file_name_conv_rate = [file_ for file_ in path_data_raw.parent.glob("currency*")][0]
raw_conv_rate = pd.read_csv(file_name_conv_rate)
raw_conv_rate_group = raw_conv_rate.groupby("time")


if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Start cleaning procedure")
cleaned_exp_full = pd.DataFrame()
for data_path in path_data_raw.glob("*.xls*"):
    logging.info("Extract the year information")
    year = data_path.name.split("_")[-1].split(".")[0]

    if int(year) > 2010:
        sheet_exp = "Table 10.1"
        sheet_household_size = "Table 10.2"
        skiprow_num = 5
    else:
        sheet_exp = "Table_21"
        sheet_household_size = "Table_22"
        skiprow_num = 4

    logging.info(f"Load the expenditure data sheet for year {year}")
    raw_exp = (
        pd.read_excel(
            data_path,
            sheet_name=sheet_exp,
            skiprows=skiprow_num,
            header=0,
            dtype=str,
        )
        .rename(columns={"Unnamed: 0": "item"})
        .dropna(how="all")
    ).reset_index(drop=True)

    logging.info(f"Load the household characteristic sheet for year {year}")
    raw_hh_size = (
        pd.read_excel(
            data_path,
            sheet_name=sheet_household_size,
            skiprows=skiprow_num,
            header=0,
            dtype=str,
        )
        .rename(columns={"Unnamed: 0": "item"})
        .dropna(how="all")
    ).reset_index(drop=True)

    index_exp_category_0 = list(raw_exp["item"]).index(
        "Current housing costs (selected dwelling)"
    )
    index_exp_category_all = list(raw_exp["item"]).index(
        "Total goods and services expenditure"
    )
    index_range_exp = [
        i for i in range(index_exp_category_0, index_exp_category_all + 1)
    ]

    index_household_size = (
        list(raw_hh_size["item"]).index("Estimated number in population") - 1
    )

    logging.info("Extract data based on unique age cohorts")
    raw_exp_unique = pd.DataFrame(
        {"item": ["household_size"] + list(raw_exp.iloc[index_range_exp, 0])}
    )
    for pos, age_cohort_true in enumerate(raw_exp.columns.str.contains("5")):
        if age_cohort_true:
            age_cohort_original = list(raw_exp.columns)[pos]

            if int(age_cohort_original[:2]) < 65:
                age_cohort_ = (
                    f"{int(age_cohort_original[:2])}-{int(age_cohort_original[:2])+9}"
                )
            else:
                if int(year) > 2010:
                    age_cohort_original = "Total"
                    pos = list(raw_exp.columns).index(age_cohort_original)
                    age_cohort_ = "65+"
                else:
                    age_cohort_ = "65+"

            raw_exp_unique[age_cohort_] = [
                raw_hh_size.loc[index_household_size, age_cohort_original]
            ] + list(raw_exp.iloc[index_range_exp, pos])

            del age_cohort_

    raw_exp_unique = raw_exp_unique.reset_index(drop=True)
    del raw_exp
    if int(year) > 2010:
        logging.info("Aggregate some types of expenditure")
        exp_cats_agg = [
            {"parent": "Household furnishings and equipment", "child": "Communication"},
            {"parent": "Miscellaneous goods and services", "child": "Education"},
        ]
        for exp_cat_agg in exp_cats_agg:
            exp_cat_parent = exp_cat_agg["parent"]
            exp_cat_child = exp_cat_agg["child"]

            index_parent = raw_exp_unique[
                raw_exp_unique["item"] == exp_cat_parent
            ].index[0]
            index_child = raw_exp_unique[raw_exp_unique["item"] == exp_cat_child].index[
                0
            ]

            raw_exp_unique.loc[index_parent, raw_exp_unique.columns[1:]] = np.array(
                raw_exp_unique.loc[index_parent, raw_exp_unique.columns[1:]],
                dtype=float,
            ) + np.array(
                raw_exp_unique.loc[index_child, raw_exp_unique.columns[1:]], dtype=float
            )
            raw_exp_unique = raw_exp_unique.drop(index_child).reset_index(drop=True)
            del index_parent, index_child, exp_cat_parent, exp_cat_child

    logging.info("Calculate the per-capita expenditure")
    cleaned_exp_per_capita = pd.DataFrame()
    for pos, column in enumerate(raw_exp_unique.columns):
        if pos >= 1:
            raw_exp_unique[column] = raw_exp_unique[column].astype(float)

            exp_age_cohort = raw_exp_unique.iloc[1:, [0, pos]]
            househouse_size = raw_exp_unique.iloc[0, pos]

            raw_conv_rate_ = raw_conv_rate_group.get_group(int(year))[
                f"{country}, convert rate local currency to 2005 usd"
            ].values[0]
            exp_age_cohort["value"] = (
                np.array(exp_age_cohort[column])
                / househouse_size
                * 365
                / 7
                * raw_conv_rate_
            )  # convert to annual expenditure, and to 2005 usd

            exp_age_cohort["value"] = pd.to_numeric(
                exp_age_cohort["value"], errors="coerce"
            )
            exp_age_cohort["age_cohort"] = column
            exp_age_cohort["household_size"] = househouse_size

            cleaned_exp_per_capita = pd.concat(
                [cleaned_exp_per_capita, exp_age_cohort.drop(columns=[column])]
            )
            del exp_age_cohort, househouse_size, pos, raw_conv_rate_
    cleaned_exp_per_capita["time"] = year
    cleaned_exp_per_capita["unit"] = "2005 usd"

    logging.info("Concat cleaned data from different years")
    cleaned_exp_full = pd.concat([cleaned_exp_full, cleaned_exp_per_capita])
    del cleaned_exp_per_capita


# save data
logging.info("Save cleaned data")
cleaned_exp_full["country"] = country
file_name = f"{data_source}_household_data_all_in_2005_usd.csv"
cleaned_exp_full.to_csv(path_data_clean / file_name, index=False)
