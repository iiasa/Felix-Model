# -*- coding: utf-8 -*-
"""
Created: Wed 27 August 2025
Description: Scripts to aggregate fertilizer use data from IFA to FeliX regions
Scope: FeliX model regionalization, module fertilizer
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
current_module = "fertilizer"

logging.info("Config data variable")
data_variable = "fertilizer_use"
data_source = "ifa"
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
raw_fertilizer = pd.DataFrame()
for raw_data_file in raw_data_info["data_file"]:
    logging.info(f"Read raw data of {raw_data_file}")
    raw_fertilizer_data = pd.read_excel(
        path_data_raw / raw_data_file,
        skiprows=2,
        # encoding="latin1",
    )

    raw_fertilizer = pd.concat(
        [raw_fertilizer, raw_fertilizer_data],
        ignore_index=True,
    )
    del raw_fertilizer_data

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_fertilizer_dep = pd.DataFrame()
    logging.info("3 data source is iea")


logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")


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
    raw_data.columns = raw_data.columns.str.lower()
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

    logging.info("Specify available years")
    years = [int(year_) for year_ in np.unique(raw_data_merge["year"])]
    logging.info("Specify the fertilizer types")
    fertilizer_types = list(np.unique(raw_data_merge["product"]))

    raw_data_merge_groups = raw_data_merge.groupby(
        [
            "un_region",
            "product",
            "year",
        ]
    )
    cleaned_fertilizer = []
    for region in regions:
        for fertilizer_type in fertilizer_types:
            for year in years:
                try:
                    raw_data_region = raw_data_merge_groups.get_group(
                        (region, fertilizer_type, year)
                    )
                except KeyError:
                    continue

                entry = {
                    "region": region,
                    "fertilizer_type": fertilizer_type,
                    "year": year,
                    "value": raw_data_region["consumption"].sum()
                    * 1000,  # convert unit from thousand tonnes to ton
                    "unit": "tonnes",
                }

                cleaned_fertilizer.append(entry)
                del entry, year, raw_data_region

    cleaned_fertilizer = pd.DataFrame(cleaned_fertilizer)

    return cleaned_fertilizer


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1950,1951,1952,...
        Fertilizer type[Africa],x,x,x,...
        Fertilizer type[AsiaPacific],x,x,x,...
        ...
        Fertilizer type[WestEu],x,x,x,...
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
    logging.info("Specify available years")
    years = np.unique(clean_data["year"])

    logging.info("Specify fertilizer types")
    fertilizer_types = np.unique(clean_data["fertilizer_type"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["fertilizer_type", "region", "year"])
    structured_data = []

    for fertilizer_type in fertilizer_types:
        for region in regions:
            entry = {
                "parameter (unit: tonnes)": f"{fertilizer_type}[{region}]",
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_fertilizer = clean_data_groups.get_group(
                            (fertilizer_type, region, year)
                        )

                        entry[year] = cleaned_fertilizer["value"].values[0]
                    except KeyError:
                        entry[year] = np.nan
                else:
                    entry[year] = np.nan

                del year

            structured_data.append(entry)
            del entry

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_fertilizer = data_cleaning(raw_fertilizer, concordance_table)

logging.info(f"Start restructuring the cleaned data")
restructured_fertilizer = data_restructure(cleaned_fertilizer)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_fertilizer.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
