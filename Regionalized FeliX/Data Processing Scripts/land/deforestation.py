# -*- coding: utf-8 -*-
"""
Created: Thur 11 Sept 2025
Description: Scripts to aggregate crop land areas to FeliX regions
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
from netCDF4 import Dataset
import xarray as xr

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
data_variable = "deforestation"
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
raw_deforestation = pd.DataFrame()
for raw_data_file in raw_data_info["data_file"]:
    logging.info(f"Read raw data of {raw_data_file}")
    raw_deforestation_ = Dataset(path_data_raw / raw_data_file, mode="r")
    # # print(raw_deforestation_)  # Metadata about the file
    # print(raw_deforestation_.variables["primf"][:])

    import matplotlib.pyplot as plt

    raw_deforestation_ = xr.open_dataset(path_data_raw / raw_data_file)
    # print(raw_deforestation_.variables)
    # print(raw_deforestation_.variables["time"][:])
    raw_deforestation_["primf"].isel(time=1165).plot()
    plt.show()
    exit()
# raw_deforestation_ = pd.read_csv(
#     path_data_raw / raw_data_file,
#     encoding="latin1",
# ).rename(columns={"Area": "country"})

# raw_deforestation = pd.concat(
#     [raw_deforestation, raw_deforestation_],
#     ignore_index=True,
# )
# del raw_deforestation_

exit()

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
    years = np.unique(raw_data_merge["year"])

    raw_data_merge_groups = raw_data_merge.groupby(["un_region", "year"])
    cleaned_deforestation = []
    for region in regions:
        num_country_ = num_country[region]
        for year in years:
            try:
                raw_data_region_year = raw_data_merge_groups.get_group((region, year))
            except KeyError:
                continue

            if len(raw_data_region_year) / num_country_ > 0.85:
                entry = {
                    "region": region,
                    "year": year,
                    "value": raw_data_region_year["value"].sum()
                    * 1000,  # convert unit from 1000 ha to ha
                    "unit": "ha",
                }

                cleaned_deforestation.append(entry)
                del (entry, raw_data_region_year)

    cleaned_deforestation = pd.DataFrame(cleaned_deforestation)

    return cleaned_deforestation


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1900,1901,1902,...
        Land Allocated for Food Crops[Africa],x,x,x,...
        Land Allocated for Food Crops[AsiaPacific],x,x,x,...
        ...
        Land Allocated for Food Crops[WestEu],x,x,x,...
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

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "year"])
    structured_data = []
    for region in regions:
        entry = {
            "parameter (unit: ha)": f"Land Allocated for Food Crops[{region}]",
        }
        for year in range(1900, 2101):
            if year in years:
                try:
                    cleaned_deforestation = clean_data_groups.get_group((region, year))

                    entry[year] = cleaned_deforestation["value"].values[0]
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
cleaned_deforestation = data_cleaning(raw_deforestation, concordance_table)

logging.info(f"Start restructuring the cleaned data")
restructured_deforestation = data_restructure(cleaned_deforestation)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_deforestation.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
