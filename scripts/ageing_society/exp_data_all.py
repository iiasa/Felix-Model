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
path_data_raw = data_home / "raw_data" / current_module / current_version
path_data_clean = data_home / "clean_data" / current_module / current_version

logging.info("Load the concordance table")
concordance_file = "exp_category_to_felix_category.csv"
concordance = pd.read_csv(path_data_raw / "concordance" / concordance_file)

logging.info("Specify unit")
unit = "2005 usd"

logging.info("Load population data")
path_population_clean = (
    data_home / "clean_data" / "felix_regionalization" / "v.11.2023" / "population"
)
population_file_name = "population_by_location_and_age_cohort_for_ageing_society.csv"
cleaned_population = pd.read_csv(path_population_clean / population_file_name)
cleaned_population_groups = cleaned_population.groupby(
    ["location", "felix_age_cohort", "year"]
)
age_cohorts_full = [f"{i}-{i+4}" for i in range(0, 100, 5)] + ["100+"]
age_cohort_all_to_felix = dict(
    zip(
        age_cohorts_full,
        ["0-14"] * 3
        + ["15-24"] * 2
        + ["25-44"] * 4
        + ["45-64"] * 4
        + ["65+"] * (len(age_cohorts_full) - 3 - 2 - 4 - 4),
    )
)


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
    if "Kosovo" in countries:
        countries.remove("Kosovo")
    age_cohorts = kwargs_info["age_cohort"]
    years = kwargs_info["time"]
    extra_attri = kwargs_info["extra_attri"]

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


def merge_age_cohort(
    cleaned_exp: pd.DataFrame,
    **kwargs,
):
    """
    To convert age cohorts by different data sources into the FeliX age cohorts

    Parameter
    ---------
    cleaned_exp: pd.DataFrame
        Cleaned expenditure data from different sources

    **kwargs
        Other arguments that may be used to merge expenditure categories

    Returns
    -------
    Age cohorts converted data in pd.Dataframe
    """
    countries = kwargs_info["country"]
    if "Kosovo" in countries:
        countries.remove("Kosovo")
    age_cohorts = kwargs_info["age_cohort"]
    years = kwargs_info["time"]
    extra_attri = kwargs_info["extra_attri"]
    felix_items = list(np.unique(cleaned_exp["felix_item"]))

    def match_age_cohort(age_cohorts_source: list):
        """
        To first check whether source age cohort need to be converted to felix age cohort.
        If yes, to match which felix age cohorts are related

        Input parameters:
            age_cohort_source: str
                Source age cohort from different data sources

        Return:
            cleaned_exp_age_converted:pd.DataFrame
                Age cohort converted cleaned expenditure data

        """
        matched_age_cohorts = []
        min_age_ = 150
        for age_cohort_ in age_cohorts_source:
            if "+" in age_cohort_:
                age_min = int(age_cohort_[:2])
                age_max = 100
                matched_cohorts = [f"{i}-{i+4}" for i in range(age_min, age_max, 5)] + [
                    "100+"
                ]
            else:
                age_min = int(age_cohort_.split("-")[0])
                age_max = int(age_cohort_.split("-")[1])
                matched_cohorts = [f"{i}-{i+4}" for i in range(age_min, age_max, 5)]

            for matched_cohort_ in matched_cohorts:
                matched_age_cohorts.append(
                    {
                        "age_cohort": age_cohort_,
                        "full_age_cohort": matched_cohort_,
                        "felix_age_cohort": age_cohort_all_to_felix[matched_cohort_],
                    }
                )

            if age_min < min_age_:
                min_age_ = age_min
                min_age_cohort_ = age_cohort_

        if age_min > 0:
            matched_cohorts = [f"{i}-{i+4}" for i in range(0, min_age_, 5)]
            for matched_cohort_ in matched_cohorts:
                matched_age_cohorts.append(
                    {
                        "age_cohort": min_age_cohort_,
                        "full_age_cohort": matched_cohort_,
                        "felix_age_cohort": age_cohort_all_to_felix[matched_cohort_],
                    }
                )

        logging.info("Match source age cohort into all exiting cohorts")
        matched_age_cohorts = pd.DataFrame(matched_age_cohorts)
        return matched_age_cohorts

    logging.info("Match source age cohorts to all age cohorts")
    matched_age_cohorts = match_age_cohort(age_cohorts)

    logging.info("Convert expenditure data into all age cohorts")
    cleaned_exp_age_merge = pd.merge(
        cleaned_exp,
        matched_age_cohorts,
        on="age_cohort",
    ).rename(columns={"age_cohort": "source_age_cohort"})
    del cleaned_exp

    felix_age_cohorts = list(np.unique(cleaned_exp_age_merge["felix_age_cohort"]))
    del age_cohorts

    cleaned_exp_groups = cleaned_exp_age_merge.groupby(
        ["country", "felix_item", "felix_age_cohort", "time"]
    )
    cleaned_data = []
    for country in countries:
        for felix_item in felix_items:
            for felix_age_cohort in felix_age_cohorts:
                for year in years:
                    cleaned_exp_region_year = cleaned_exp_groups.get_group(
                        (
                            country,
                            felix_item,
                            felix_age_cohort,
                            year,
                        )
                    )

                    if (
                        len(np.unique(cleaned_exp_region_year["source_age_cohort"]))
                        == 1
                    ):
                        if extra_attri:
                            # extra attribute is another column in the cleaned data
                            entry = {
                                "country": country.lower(),
                                "felix_item": felix_item,
                                "felix_age_cohort": felix_age_cohort,
                                "time": year,
                                extra_attri[0]: cleaned_exp_region_year[
                                    extra_attri[0]
                                ].values[0],
                                "value": cleaned_exp_region_year["value"].values[0],
                                "unit": unit,
                            }
                        else:
                            entry = {
                                "country": country.lower(),
                                "felix_item": felix_item,
                                "felix_age_cohort": felix_age_cohort,
                                "time": year,
                                "value": cleaned_exp_region_year["value"].values[0],
                                "unit": unit,
                            }
                    else:
                        if country == "Kosovo":
                            continue

                        cleaned_population_region_year = (
                            cleaned_population_groups.get_group(
                                (
                                    country.lower(),
                                    felix_age_cohort,
                                    year,
                                )
                            ).rename(
                                columns={
                                    "age": "full_age_cohort",
                                    "value": "value_population",
                                }
                            )
                        )

                        cleaned_exp_pop_merge = pd.merge(
                            cleaned_exp_region_year,
                            cleaned_population_region_year[
                                ["full_age_cohort", "value_population"]
                            ],
                            on="full_age_cohort",
                        )

                        if extra_attri:
                            # extra attribute is another column in the cleaned data
                            entry = {
                                "country": country.lower(),
                                "felix_item": felix_item,
                                "felix_age_cohort": felix_age_cohort,
                                "time": year,
                                extra_attri[0]: (
                                    np.array(cleaned_exp_pop_merge[extra_attri[0]])
                                    * np.array(
                                        cleaned_exp_pop_merge["value_population"]
                                    )
                                ).sum()
                                / cleaned_exp_pop_merge["value_population"].sum(),
                                "value": (
                                    np.array(cleaned_exp_pop_merge["value"])
                                    * np.array(
                                        cleaned_exp_pop_merge["value_population"]
                                    )
                                ).sum()
                                / cleaned_exp_pop_merge["value_population"].sum(),
                                "unit": unit,
                            }
                        else:
                            entry = {
                                "country": country.lower(),
                                "felix_item": felix_item,
                                "felix_age_cohort": felix_age_cohort,
                                "time": year,
                                "value": (
                                    np.array(cleaned_exp_pop_merge["value"])
                                    * np.array(
                                        cleaned_exp_pop_merge["value_population"]
                                    )
                                ).sum()
                                / cleaned_exp_pop_merge["value_population"].sum(),
                                "unit": unit,
                            }

                    cleaned_data.append(entry)
                    del entry, cleaned_exp_region_year

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"time": "int"})

    return cleaned_data


logging.info("Convert the item categories and restructure the data")
exp_data_all_sources = pd.DataFrame()
for data_source in path_data_clean.glob("**/*"):
    if data_source.is_dir():
        for data_file in data_source.glob("*_in_2005_usd.csv"):
            logging.info("Read cleaned expenditure data")
            cleaned_exp = pd.read_csv(data_file)

            logging.info("Configure basic information of the expenditure dataset")
            exp_items = list(np.unique(cleaned_exp["item"]))
            age_cohorts = list(np.unique(cleaned_exp["age_cohort"]))
            years = list(set(cleaned_exp["time"]))
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
            }
            logging.info("Convert source item categories into felix categories")
            cleaned_exp_aggregated = merge_exp_category(
                cleaned_exp=cleaned_exp,
                concordance=concordance,
                kwargs_info=kwargs_info,
            )

            logging.info("Convert source age cohorts into felix age cohorts")
            cleaned_age_cohort = merge_age_cohort(
                cleaned_exp=cleaned_exp_aggregated,
                kwargs_info=kwargs_info,
            )

            exp_data_all_sources = pd.concat(
                [exp_data_all_sources, cleaned_age_cohort], ignore_index=True
            ).reset_index(drop=True)


#             logging.info("Restructure the data")
#             felix_items = np.unique(cleaned_exp_aggregated["felix_item"])
#             del exp_items, cleaned_exp

#             cleaned_exp_groups = cleaned_exp_aggregated.groupby(
#                 ["country", "felix_item", "age_cohort", "time"]
#             )
#             for country in countries:
#                 for exp_item in felix_items:
#                     for age_cohort in age_cohorts:
#                         entry = {
#                             "country": country,
#                             "felix_item": exp_item.lower(),
#                             "age_cohort": age_cohort,
#                             "unit": unit,
#                         }
#                         if exp_item == "total":
#                             logging.info("Restructure the extra attribute as well")
#                             if extra_attri:
#                                 entry_extra_attri = {
#                                     "country": country,
#                                     "felix_item": extra_attri[0].lower(),
#                                     "age_cohort": age_cohort,
#                                 }
#                         for year in range(1900, 2101):
#                             if year in years:
#                                 try:
#                                     cleaned_exp_ = cleaned_exp_groups.get_group(
#                                         (country, exp_item, age_cohort, year)
#                                     )
#                                     value = cleaned_exp_["value"].values[0]

#                                     if exp_item == "total":
#                                         if extra_attri:
#                                             extra_attri_value = cleaned_exp_[
#                                                 extra_attri[0]
#                                             ].values[0]
#                                         else:
#                                             extra_attri_value = None
#                                 except KeyError:
#                                     value = None
#                                     extra_attri_value = None
#                             else:
#                                 value = None
#                                 extra_attri_value = None

#                             entry[year] = value
#                             if exp_item == "total":
#                                 if extra_attri:
#                                     entry_extra_attri[year] = extra_attri_value

#                             del year

#                         exp_data_all_sources.append(entry)
#                         del entry
#                         if exp_item == "total":
#                             if extra_attri:
#                                 exp_data_all_sources.append(entry_extra_attri)
#                                 del entry_extra_attri
# exp_data_all_sources = pd.DataFrame(exp_data_all_sources).replace(0, np.nan)

# save data
logging.info("Save cleaned data")
exp_data_all_sources = exp_data_all_sources[exp_data_all_sources["value"] != 0]
file_name = f"personal_data_in_felix_classification.csv"
exp_data_all_sources.to_csv(path_data_clean / file_name, index=False)
