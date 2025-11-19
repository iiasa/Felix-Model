# -*- coding: utf-8 -*-
"""
Created: Mon 27 May 2024
Description: Scripts to capital intensity of economic output for 5 FeliX regions
Scope: FeliX model regionalization, module capital 
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import json
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyam
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of capital
# data sources are wittgensteinwittgenstein, world_bank, unpd, ggdc (Groningen Growth and Development Center)
data_source = "ggdc"
variable = "capital_intensity"
download_method = "manually"

# read config.yaml file
yaml_dir = Path("scripts/capital/config.yaml")
with open(yaml_dir, "r") as dimension_file:
    data_info = yaml.safe_load(dimension_file)

version = data_info["version"]
felix_module = data_info["module"]
# Any path consists of at least a root path, a version path, a module path
path_clean_data_folder = Path(data_info["data_output"]["root_path"]).joinpath(
    f"version_{version}/{felix_module}"
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
    if (
        (dataset["datasource"] == data_source)
        and (dataset["variable"] == variable)
        and (dataset["download_via"] == download_method)
    ):
        raw_data_info = dataset
if not raw_data_info:
    logging.warning(f"No {felix_module} data from {data_source} configed in .yaml")
    raise KeyError
logging.info("Information of input data is loaded")

logging.info("Set path to the raw data")
# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = Path(raw_data_info["root_path"]).joinpath(
    f"version_{version}/{felix_module}"
)
raw_data_files = raw_data_info["data_file"]
logging.info("Path to raw data set")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path(raw_data_info["root_path"]).joinpath(
    f"version_{version}/concordance"
)
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")

regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_capital = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "ggdc":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "manually":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            raw_capital_ = pd.read_excel(
                path_raw_data_folder / raw_data_file,
                sheet_name="Data",
            )

            raw_capital = pd.concat(
                [
                    raw_capital,
                    raw_capital_[
                        [
                            "country",
                            "year",
                            "ck",  # Capital stock at current PPPs(in mil. 2005US$)
                            "cgdpo",  # Output-side real GDP at current PPPs (in mil. 2005US$)
                        ]
                    ],
                ],
                ignore_index=True,
            )
            del raw_capital_
        else:
            logging.warning("The download method is not API")
            raise KeyError

logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


# Define data cleaning function
def data_cleaning(
    raw_data: pd.DataFrame,
    raw_data_source: str,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from each data source

    raw_data_source: str
        The data source of raw data

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
    if raw_data_source == "wittgenstein":
        cleaned_data = data_cleaning_wittgenstein(
            raw_data=raw_data,
            concordance=concordance,
            kwargs=kwargs,
        )
    elif raw_data_source == "world_bank":
        cleaned_data = data_cleaning_world_bank(
            raw_data=raw_data,
            concordance=concordance,
            kwargs=kwargs,
        )
    elif raw_data_source == "unpd":
        cleaned_data = data_cleaning_unpd(
            raw_data=raw_data,
            concordance=concordance,
            kwargs=kwargs,
        )
    elif raw_data_source == "ggdc":
        cleaned_data = data_cleaning_ggdc(
            raw_data=raw_data,
            concordance=concordance,
            kwargs=kwargs,
        )

    return cleaned_data


def data_cleaning_wittgenstein(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    print("this is the data_cleaning_wittgenstein")


def data_cleaning_world_bank(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from World Bank to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from World Bank

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
    print("this is the data_cleaning_wittgenstein")


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
    print("this is the data_cleaning_unpd")


def data_cleaning_ggdc(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from Groningen Growth and Development Center (ggdc) to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from GGDC

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
    if download_method == "manually":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", "un_region"]],
            on="country",
        )
    else:
        raise KeyError("No capital-related data downloaded via API")

    raw_data_groups = raw_data_merge.groupby(["un_region"])
    cleaned_data = pd.DataFrame()
    for region in regions:
        cleaned_data_region = pd.DataFrame()
        if region != "World":  # distinguish the region
            raw_data_region = raw_data_groups.get_group(region).dropna()
            years = np.unique(raw_data_region["year"])
            countries = np.unique(raw_data_region["country"])

            raw_data_region_groups = raw_data_region.groupby("year")
            for year in years:
                if year <= 1990:
                    raw_data_region_year = raw_data_region_groups.get_group(year)
                    if len(raw_data_region_year) == len(countries):
                        cleaned_data_region = pd.concat(
                            [
                                cleaned_data_region,
                                raw_data_region_year,
                            ],
                            ignore_index=True,
                        )
                    del raw_data_region_year

            entry = [
                {
                    "un_region": region,
                    "value": cleaned_data_region["ck"].sum()
                    / cleaned_data_region["cgdpo"].sum(),
                }
            ]
            cleaned_data = pd.concat(
                [cleaned_data, pd.DataFrame(entry)],
                ignore_index=True,
            )
            del (
                raw_data_region,
                years,
                countries,
                cleaned_data_region,
                entry,
                raw_data_region_groups,
            )
        else:
            years = np.unique(raw_data_merge["year"])
            countries = np.unique(raw_data_merge["country"])

            raw_data_merge_groups = raw_data_merge.groupby("year")
            for year in years:
                if year <= 1990:
                    raw_data_year = raw_data_merge_groups.get_group(year)
                    if len(raw_data_year) == len(countries):
                        cleaned_data_region = pd.concat(
                            [
                                cleaned_data_region,
                                raw_data_year,
                            ],
                            ignore_index=True,
                        )
                    del raw_data_year

            entry = [
                {
                    "un_region": region,
                    "value": cleaned_data_region["ck"].sum()
                    / cleaned_data_region["cgdpo"].sum(),
                }
            ]
            cleaned_data = pd.concat(
                [cleaned_data, pd.DataFrame(entry)],
                ignore_index=True,
            )
            del years, countries, cleaned_data_region, entry, raw_data_merge_groups

    return cleaned_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_capital = data_cleaning(
    raw_data=raw_capital,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

logging.info(f"Start writing the cleaned data")
cleaned_capital.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the cleaned data")
logging.info(f"The whole procedures of cleaning data are done!")
