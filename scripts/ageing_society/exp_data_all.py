"""
Created: Monday 24 Feb 2025
Description: Scripts to combine all expenditure data from different data sources
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

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version
path_data_clean = data_home / "clean_data" / current_module / current_version

logging.info("Load the concordance table")
concordance_file = "exp_category_to_felix_category.csv"
concordance = pd.read_csv(path_data_raw / "concordance" / concordance_file)


def merge_exp_category(
    cleaned_exp: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To merge expenditure categories by different data sources into the FeliX categories

    Parameter
    ---------
    cleaned_exp: pd.DataFrame
        Cleaned expenditure data from different sources

    Concordance: pd.DataFrame
        The concordance table that links source expenditure categories and FeliX categories

    **kwargs
        Other arguments that may be used to merge expenditure categories

    Returns
    -------
    mergeed data in pd.Dataframe
    """
    cleaned_exp["item"] = [item_.lower() for item_ in cleaned_exp["item"]]
    cleaned_exp_merge = pd.merge(
        cleaned_exp,
        concordance[["item", "felix_item"]],
        on="item",
    )
    del cleaned_exp

    years = np.unique(cleaned_exp_merge["time"])

    felix_items = list(np.unique(cleaned_exp_merge["felix_item"]))
    countries = kwargs_info["country"]
    age_cohorts = kwargs_info["age_cohort"]
    years = kwargs_info["time"]
    extra_attri = kwargs_info["extra_attri"]
    unit = kwargs_info["unit"]

    cleaned_exp_groups = cleaned_exp_merge.groupby(
        ["country", "felix_item", "age_cohort", "time"]
    )
    cleaned_data = []
    for country in countries:
        for felix_item in felix_items:
            for age_cohort in age_cohorts:
                for year in years:
                    cleaned_exp_region_year = cleaned_exp_groups.get_group(
                        (
                            country,
                            felix_item,
                            age_cohort,
                            year,
                        )
                    )
                    if extra_attri:
                        # extra attribute is another column in the cleaned data
                        entry = {
                            "country": country,
                            "felix_item": felix_item,
                            "age_cohort": age_cohort,
                            "time": year,
                            extra_attri[0]: cleaned_exp_region_year[
                                extra_attri[0]
                            ].values[0],
                            "value": cleaned_exp_region_year["value"].sum(),
                            "unit": unit,
                        }
                    else:
                        entry = {
                            "country": country,
                            "felix_item": felix_item,
                            "age_cohort": age_cohort,
                            "time": year,
                            "value": cleaned_exp_region_year["value"].sum(),
                            "unit": unit,
                        }

                    cleaned_data.append(entry)

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"time": "int"})

    return cleaned_data


logging.info("Convert the item categories and restructure the data")
exp_data_all_sources = []
for data_source in path_data_clean.glob("**/*"):
    if data_source.is_dir():
        for data_file in data_source.glob("*_data_all*"):
            logging.info("Read cleaned expenditure data")
            cleaned_exp = pd.read_csv(data_file)

            logging.info("Configure basic information of the expenditure dataset")
            exp_items = list(np.unique(cleaned_exp["item"]))
            age_cohorts = list(np.unique(cleaned_exp["age_cohort"]))
            years = list(set(cleaned_exp["time"]))
            unit = list(np.unique(cleaned_exp["unit"]))[0]
            countries = list(np.unique(cleaned_exp["country"]))

            logging.info("An extra attribute may exist, such as household size")
            extra_attri = list(
                set(cleaned_exp.columns)
                - set(["item", "value", "age_cohort", "unit", "time", "country"])
            )

            logging.info("Main dimensions of expenditure dataset")
            kwargs_info = {
                "time": years,
                "age_cohort": age_cohorts,
                "country": countries,
                "extra_attri": extra_attri,
                "unit": unit,
            }
            logging.info("Convert source item categories into felix categories")
            cleaned_exp_aggregated = merge_exp_category(
                cleaned_exp=cleaned_exp,
                concordance=concordance,
                kwargs_info=kwargs_info,
            )

            logging.info("Restructure the data")
            felix_items = np.unique(cleaned_exp_aggregated["felix_item"])
            del exp_items, cleaned_exp

            cleaned_exp_groups = cleaned_exp_aggregated.groupby(
                ["country", "felix_item", "age_cohort", "time"]
            )
            for country in countries:
                for exp_item in felix_items:
                    for age_cohort in age_cohorts:
                        entry = {
                            "country": country,
                            "felix_item": exp_item.lower(),
                            "age_cohort": age_cohort,
                            "unit": unit,
                        }
                        if exp_item == "total":
                            logging.info("Restructure the extra attribute as well")
                            if extra_attri:
                                entry_extra_attri = {
                                    "country": country,
                                    "felix_item": extra_attri[0].lower(),
                                    "age_cohort": age_cohort,
                                }
                        for year in range(1900, 2101):
                            if year in years:
                                try:
                                    cleaned_exp_ = cleaned_exp_groups.get_group(
                                        (country, exp_item, age_cohort, year)
                                    )
                                    value = cleaned_exp_["value"].values[0]

                                    if exp_item == "total":
                                        if extra_attri:
                                            extra_attri_value = cleaned_exp_[
                                                extra_attri[0]
                                            ].values[0]
                                        else:
                                            extra_attri_value = None
                                except KeyError:
                                    value = None
                                    extra_attri_value = None
                            else:
                                value = None
                                extra_attri_value = None

                            entry[year] = value
                            if exp_item == "total":
                                if extra_attri:
                                    entry_extra_attri[year] = extra_attri_value

                            del year

                        exp_data_all_sources.append(entry)
                        del entry
                        if exp_item == "total":
                            if extra_attri:
                                exp_data_all_sources.append(entry_extra_attri)
                                del entry_extra_attri
exp_data_all_sources = pd.DataFrame(exp_data_all_sources).replace(0, np.nan)

# save data
logging.info("Save cleaned data")
file_name = f"personal_data_time_series.csv"
exp_data_all_sources.to_csv(path_data_clean / file_name, index=False)
