# -*- coding: utf-8 -*-
"""
Created: Mon 19 May 2025
Description: Scripts to aggregate urban land area from The World Bank to FeliX regions
Scope: FeliX model regionalization, module land
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import datetime
import json
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

# import pyam
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

logging.info("Configure peoject")
current_project = "felix_regionalization"

logging.info("Configure module")
current_module = "land"

logging.info("Config data variable")
data_variable = "urban_area"
data_source = "world_bank"
data_download_method = "api"

logging.info("Configure paths")
path_data_raw = (
    data_home / "raw_data" / current_project / current_version / current_module
)
path_data_clean = (
    data_home / "clean_data" / current_project / current_version / current_module
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

# read config.yaml file
yaml_dir = Path(
    f"Regionalied FeliX/Data Processing Scripts/{current_module}/config.yaml"
)
with open(yaml_dir, "r") as dimension_file:
    data_info = yaml.safe_load(dimension_file)

logging.info("Extracting information of input data")
raw_data_info = {}
for dataset in data_info["data_input"]:
    if (
        (dataset["datasource"] == data_source)
        and (dataset["variable"] == data_variable)
        and (dataset["download_via"] == data_download_method)
    ):
        raw_data_info = dataset
if not raw_data_info:
    logging.warning(f"No {current_module} data from {data_source} configed in .yaml")
    raise KeyError

logging.info("Check the dependency to clean raw data")
raw_data_dependency = raw_data_info["dependency"]
if raw_data_dependency:
    felix_module_dep = raw_data_info["dependency_module"]
    raw_data_files_dep = raw_data_info["dependency_file"]
    path_raw_data_folder_dep = path_data_raw.parent / felix_module_dep

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = path_data_raw.parent / "concordance"

concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
if "ipcc_r6" in concordance_file:
    regions = data_info["dimension"]["ipcc_r6"]
    final_region_name = "ipcc_r6"
else:
    regions = data_info["dimension"]["region"]
    final_region_name = "un_regions"
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_urban_land = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    for raw_data_file in raw_data_info["data_file"]:
        logging.info(f"Data input: {raw_data_file}")
        if data_download_method == "api":
            logging.info(f"Data input: {raw_data_file}")
            with open(path_data_raw / raw_data_file) as fact_file:
                raw_urban_land_ = json.load(fact_file)
            for data_point in raw_urban_land_:
                if data_point["value"] == "":
                    data_point["value"] = np.nan
                    del data_point

            raw_urban_land_ = pd.DataFrame(raw_urban_land_)
            raw_urban_land = pd.concat(
                [raw_urban_land, raw_urban_land_],
                ignore_index=True,
            )
            del raw_urban_land_
        else:
            logging.warning("The download method is not API")
            raise KeyError
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")

logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
# concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")
num_country = {}
for region in regions:
    num_country[region] = len(
        concordance_table.loc[concordance_table["un_region"] == region]
    )


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
    if "ipcc_r6" in concordance.columns:
        final_region_name = "ipcc_r6"
    else:
        final_region_name = "un_region"

    if data_download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["location", final_region_name]],
            left_on="country",
            right_on="location",
        ).rename(
            columns={
                "time": "year",
            }
        )
    else:
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", final_region_name]],
            on="country",
        ).rename(
            columns={
                "time": "year",
            }
        )

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby([final_region_name, "year"])
    cleaned_data = []
    for region in regions:
        num_country_ = num_country[region]
        for year in years:
            try:
                raw_data_region_year = raw_data_groups.get_group(
                    (
                        region,
                        year,
                    )
                )
            except KeyError:
                continue

            if len(raw_data_region_year) / num_country_ > 0.85:
                entry = {
                    final_region_name: region,
                    "year": year,
                    "value": raw_data_region_year["value"].sum(),
                    "unit": "km^2",
                }

                cleaned_data.append(entry)
                del entry
    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


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


# Define data restructure function
def data_restructure(
    cleaned_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
      '''
          Time,1950,1951,1952,...
          Urban and Industrial Land in km^2[Region 1],x,x,x,...
          Urban and Industrial Land in km^2[Region 2],x,x,x,...
          ......
          Urban and Industrial Land in km^2[Region N],x,x,x,...
      '''
      The restructured data will be used as historic data for data calibration


      Parameter
      ---------
      cleaned_data: pd.DataFrame
          Clenaed data via the data_cleaning function

      **kwargs
          Other arguments that may be used to restructure the clean data

      Returns
      -------
      restructured data in pd.Dataframe

    """
    logging.info("Group cleaned data by region, gender, and ages")
    year_avail = np.unique(cleaned_data["year"])

    restructured_data = []
    logging.info("Group cleaned data by all dimensions")
    if "ipcc_r6" in cleaned_data.columns:
        final_region_name = "ipcc_r6"
    else:
        final_region_name = "un_region"

    cleaned_data_groups = cleaned_data.groupby([final_region_name, "year"])
    for region in regions:
        entry = {
            "parameter: in km^2": f"Urban and Industrial Land in km^2[{region}]",
        }
        for year in range(1900, 2101):
            if year in year_avail:
                data_by_all = cleaned_data_groups.get_group(
                    (
                        region,
                        year,
                    )
                )
                index_value = [
                    pos
                    for pos, column_name in enumerate(data_by_all.columns)
                    if column_name == "value"
                ][0]
                entry[year] = data_by_all.iloc[0, index_value]
                del index_value
            else:
                entry[year] = ""

        restructured_data.append(entry)
        del entry

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_urban_land = data_cleaning(
    raw_data=raw_urban_land,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# Start restructuring cleaned data, which will be used as historic data
restructured_urban_land = data_restructure(
    cleaned_data=cleaned_urban_land,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_urban_land.to_csv(
    path_data_clean.joinpath(
        f"{data_variable}_by_time_series_{data_source}_{final_region_name}.csv"
    ),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
