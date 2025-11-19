# -*- coding: utf-8 -*-
"""
Created: Mon 28 April 2025
Description: Scripts to aggregate water withdrawal data by sector and region from IEA to FeliX regions
Scope: FeliX model regionalization, module water
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import datetime
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
current_module = "water"

logging.info("Config data variable")
data_variable = "wat_withdrawal"
data_source = "aquastat"
data_download_method = "manually"

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
yaml_dir = Path(f"scripts/{current_module}/config.yaml")
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
logging.info("Information of input data is loaded")

logging.info("Check the dependency to clean raw data")
raw_data_dependency = raw_data_info["dependency"]
if raw_data_dependency:
    felix_module_dep = raw_data_info["dependency_module"]
    raw_data_files_dep = raw_data_info["dependency_file"]
    path_raw_data_folder_dep = path_data_raw.parent.parent / felix_module_dep

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = path_data_raw.parent / "concordance"

concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_water = pd.DataFrame()
for raw_data_file in raw_data_info["data_file"]:
    logging.info(f"Read raw data of {raw_data_file}")
    raw_water_data = pd.read_csv(
        path_data_raw / raw_data_file,
        encoding="latin1",
    ).rename(columns={"Area": "country"})

    raw_water = pd.concat(
        [raw_water, raw_water_data],
        ignore_index=True,
    )
    del raw_water_data

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_water_dep = pd.DataFrame()
    logging.info("3 data source is iea")


logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


# Define cleaning function
def data_cleaning(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To transfer raw data from IEA database into FeliX region classification

    Parameter
    ---------
    raw_data: pd.DataFrame
        Data downloaded directly from IEA database.

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    cleaned data in pd.Dataframe

    """
    logging.info("Merge cleaned data with regional concordance")
    if data_download_method == "manually":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", "un_region"]],
            on="country",
        )
    else:
        logging.warning(f"Download method is not manually")
        raise KeyError

    raw_data_merge.columns = raw_data_merge.columns.str.lower()
    logging.info("Specify energy sources")
    water_withd_sectors = np.unique(raw_data_merge["variable"])

    logging.info("Specify available years")
    years = np.unique(raw_data_merge["year"])

    raw_data_merge_groups = raw_data_merge.groupby(["un_region", "variable", "year"])
    cleaned_water = []
    for region in regions:
        for water_withd_sector in water_withd_sectors:
            for year in years:
                try:
                    raw_data_region = raw_data_merge_groups.get_group(
                        (region, water_withd_sector, year)
                    )
                except KeyError:
                    continue

                entry = {
                    "region": region,
                    "water_withd_sector": water_withd_sector,
                    "year": year,
                    "value": raw_data_region["value"].sum(),
                    "unit": "10^9 m3/year",
                }

                cleaned_water.append(entry)
                del entry, year, raw_data_region

    cleaned_water = pd.DataFrame(cleaned_water)
    return cleaned_water


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1950,1951,1952,...
        Sector 1 Water Withdrawal[Africa],x,x,x,...
        Sector 1 Water Withdrawal[AsiaPacific],x,x,x,...
        ...
        Sector n Water Withdrawal[LAC],x,x,x,...
        Sector n Water Withdrawal[WestEu],x,x,x,...
    '''
    The restructured data will be used as historic data for data calibration


    Parameter
    ---------
    clean_data: pd.DataFrame
        Data cleaned via the cleaning function.

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    restructured data in pd.Dataframe

    """
    logging.info("Specify sector categories and available years")
    water_withd_sectors = np.unique(clean_data["water_withd_sector"])
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "water_withd_sector", "year"])
    structured_data = []
    for region in regions:
        for wat_withd_sector in water_withd_sectors:
            entry = {
                "parameter (unit: km3)": wat_withd_sector,
                "region": region,
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_water = clean_data_groups.get_group(
                            (region, wat_withd_sector, year)
                        )

                        entry[year] = cleaned_water["value"].values[0] * (10**9)
                    except KeyError:
                        entry[year] = np.nan
                else:
                    entry[year] = np.nan

                del year

            structured_data.append(entry)
            del entry, wat_withd_sector

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_water = data_cleaning(raw_water, concordance_table)
logging.info("Finish data cleaning")

logging.info(f"Start restructuring the cleaned data")
restructured_water = data_restructure(cleaned_water)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_water.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
