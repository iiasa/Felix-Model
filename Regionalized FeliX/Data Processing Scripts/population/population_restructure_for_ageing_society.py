# -*- coding: utf-8 -*-
"""
Created: Tue 04 March 2025
Description: Scripts to restruct population data by age cohort and country
Scope: FeliX model regionalization, module Population 
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import datetime
import logging
import re
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
current_version = os.getenv(f"CURRENT_VERSION_FELIX_REGIONALIZATION")

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of population
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "unpd"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/population/config.yaml")
with open(yaml_dir, "r") as dimension_file:
    data_info = yaml.safe_load(dimension_file)


felix_module = data_info["module"]
# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = (
    data_home / "raw_data" / "felix_regionalization" / current_version / felix_module
)
path_clean_data_folder = (
    data_home / "clean_data" / "felix_regionalization" / current_version / felix_module
)

# set logger
if not (path_clean_data_folder.joinpath("logs")).exists():
    (path_clean_data_folder.joinpath("logs")).mkdir(
        parents=True,
    )

logging.basicConfig(
    level=logging.DEBUG,
    filename=f"{path_clean_data_folder}/logs/{timestamp.strftime('%d-%m-%Y')}.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# add the handler to the root logger
logging.getLogger().addHandler(console)

logging.info("Extracting information of input data")
raw_data_info = {}
for dataset in data_info["data_input"]:
    if dataset["datasource"] == data_source:
        if dataset["download_via"] == download_method:
            raw_data_info = dataset
if not raw_data_info:
    logging.warning(f"No {felix_module} data from {data_source} configed in .yaml")
    raise KeyError
logging.info("Information of input data is loaded")

logging.info("Set path to the raw data")
# Any path consists of at least a root path, a version path, a module path
raw_data_files = raw_data_info["data_file"]
logging.info("Path to raw data set")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = path_raw_data_folder.parent / "concordance"
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
age_cohorts = data_info["dimension"]["age"]
age_cohorts_to_felix_cohort = dict(
    zip(
        [age_cohort_.replace("--", "-") for age_cohort_ in age_cohorts],
        ["0-14"] * 3
        + ["15-24"] * 2
        + ["25-44"] * 4
        + ["45-64"] * 4
        + ["65+"] * (len(age_cohorts) - 3 - 2 - 4 - 4),
    )
)

# Read raw data from UNPD
logging.info(f"Read raw data")
raw_population = pd.DataFrame()
for raw_data_file in raw_data_files:
    logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
    if download_method == "api":
        raw_population_ = pd.read_csv(
            path_raw_data_folder / raw_data_file,
        )
        raw_population = pd.concat(
            [raw_population, raw_population_],
            ignore_index=True,
        )
        del raw_population_
    else:
        logging.warning("The download method of source data is not API")

logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


def data_cleaning_unpd(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from UNPD to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from UNPD

    Concordance: pd.DataFrame
        The concordance table that links source regional classifications and FeliX regions
        The FeliX regions are Africa, AsiaPacific, EastEu, LAC (latin american and the caribbean),
        WestEu.

    **kwargs
        Other arguments that may be used to clean raw data

    Returns
    -------
    cleaned data in pd.Dataframe
    """
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["region_api", "un_region"]],
        left_on="Location",
        right_on="region_api",
    ).rename(
        columns={
            "TimeLabel": "Year",
            "Sex": "sex",
            "AgeLabel": "age",
        }
    )
    raw_data_merge["sex"] = [value.lower() for value in raw_data_merge["sex"]]

    locations = np.unique(raw_data_merge["region_api"])
    years = [
        year_
        for year_ in np.unique(raw_data_merge["Year"])
        if (int(year_) > 1980) and (int(year_) < 2026)
    ]

    raw_data_groups = raw_data_merge.groupby(["region_api", "Year"])
    cleaned_data = []
    for location in locations:
        for year in years:
            raw_data_region_year = raw_data_groups.get_group((location, year))
            felix_region = list(np.unique(raw_data_region_year["un_region"]))[0]

            raw_data_region_year_ages = (
                raw_data_region_year.groupby(["age"])["Value"].sum().reset_index()
            )
            for age in age_cohorts:
                age = age.replace("--", "-")
                entry = {
                    "location": location.lower(),
                    "felix_region": felix_region,
                    "age": age,
                    "felix_age_cohort": age_cohorts_to_felix_cohort[age],
                    "year": year,
                    "value": raw_data_region_year_ages.loc[
                        raw_data_region_year_ages["age"] == age,
                        "Value",
                    ].values[0],
                    "unit": "person",
                }

                cleaned_data.append(entry)
                del entry, age

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_population = data_cleaning_unpd(
    raw_data=raw_population,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")


logging.info(f"Writing the cleaned data")
cleaned_population.to_csv(
    path_clean_data_folder.joinpath(
        f"population_by_location_and_age_cohort_for_ageing_society.csv"
    ),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
