# -*- coding: utf-8 -*-
"""
Created: Wed 24 Sept 2025
Description: Scripts to calculate total grassland for livestock farming from Land Use Harmonization to FeliX regions
Scope: FeliX model regionalization, module land
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
current_module = "land"

logging.info("Config data variable")
data_variable = "grass_land_area"
data_source = "land_use_harmonization"
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
yaml_dir = Path(
    f"Regionalized FeliX/Data Processing Scripts/{current_module}/config.yaml"
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
logging.info("Information of input data is loaded")

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
regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_grassland = pd.DataFrame()
for raw_data_file in raw_data_info["data_file"]:
    logging.info(f"Read raw data of {raw_data_file}")
    raw_grassland_data = pd.read_csv(
        path_data_raw / raw_data_file,
        encoding="latin1",
    )

    raw_grassland = pd.concat(
        [raw_grassland, raw_grassland_data],
        ignore_index=True,
    )
    del raw_grassland_data

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    logging.info("Raw data dependence exists")


logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")
num_country = {}
for region in regions:
    num_country[region] = len(
        concordance_table.loc[concordance_table["un_region"] == region]
    )


raw_grassland.columns = raw_grassland.columns.str.lower()
logging.info("Merge cleaned data with regional concordance")
if data_download_method == "manually":
    raw_grassland_merge = pd.merge(
        raw_grassland,
        concordance_table[["country", "un_region"]],
        on="country",
    )
else:
    logging.warning(f"Download method is not manually")
    raise KeyError

raw_grassland_merge_groups = (
    raw_grassland_merge.groupby("un_region").sum(numeric_only=True) * 100
)  # convert unit from km2 to ha
raw_grassland_merge_groups.drop(columns=["country_code"], inplace=True)


logging.info("Write clean data into a .csv file")
raw_grassland_merge_groups.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
