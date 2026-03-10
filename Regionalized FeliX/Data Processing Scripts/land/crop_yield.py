# -*- coding: utf-8 -*-
"""
Created: Thur 11 September 2025
Description: Scripts to calculate crop yields from FAOSTAT to FeliX regions
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
data_variable = "crop_yield"
data_source = "faostat"
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
raw_crop_data = pd.DataFrame()
for raw_data_file in raw_data_info["data_file"]:
    logging.info(f"Read raw data of {raw_data_file}")
    raw_crop_data_ = pd.read_csv(
        path_data_raw / raw_data_file,
        encoding="latin1",
    ).rename(columns={"Area": "country"})

    raw_crop_data = pd.concat(
        [raw_crop_data, raw_crop_data_],
        ignore_index=True,
    )

    del raw_crop_data_

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

    def name_mapping(crop_name: str):
        if crop_name in [
            "Pulses, Total",
        ]:
            return "Pulses"
        elif crop_name in [
            "Cereals, primary",
        ]:
            return "Grains"
        elif crop_name in [
            "Citrus Fruit, Total",
            "Fruit Primary",
            "Roots and Tubers, Total",
            "Vegetables Primary",
        ]:
            return "VegFruits"
        elif crop_name in [
            "Oilcrops, Cake Equivalent",
            "Oilcrops, Oil Equivalent",
            "Sugar Crops Primary",
            "Treenuts, Total",
        ]:
            return "OtherCrops"

    raw_data_merge["food_category"] = (
        raw_data_merge["item"].astype(str).map(name_mapping)
    )
    food_categories = ["Pulses", "Grains", "VegFruits", "OtherCrops"]

    raw_data_merge_groups = raw_data_merge.groupby(
        ["un_region", "element", "year", "food_category"]
    )

    cleaned_crop_data = []
    for region in regions:
        for food_category in food_categories:
            for year in years:
                try:
                    raw_crop_production = raw_data_merge_groups.get_group(
                        (region, "Production", year, food_category)
                    )
                except KeyError:
                    continue

                try:
                    raw_crop_area = raw_data_merge_groups.get_group(
                        (region, "Area harvested", year, food_category)
                    )
                except KeyError:
                    continue

                raw_crop_yield_merge = pd.merge(
                    raw_crop_production[
                        ["country", "un_region", "year", "item", "value"]
                    ].rename(columns={"value": "production"}),
                    raw_crop_area[["country", "item", "value"]].rename(
                        columns={"value": "harvested_area"}
                    ),
                    on=["country", "item"],
                )

                if food_category == "OtherCrops":
                    raw_crop_area_tot = (
                        raw_crop_yield_merge["harvested_area"].sum()
                        - raw_crop_yield_merge.loc[
                            raw_crop_yield_merge["item"] == "Oilcrops, Cake Equivalent",
                            "harvested_area",
                        ].sum()
                    )  # avoid double accounting for oil crops
                else:
                    raw_crop_area_tot = raw_crop_yield_merge["harvested_area"].sum()

                entry_yield = {
                    "region": region,
                    "food_category": food_category,
                    "year": year,
                    "yield": raw_crop_production["value"].sum() / raw_crop_area_tot,
                    "harvested_area": raw_crop_area_tot,
                }

                cleaned_crop_data.append(entry_yield)
                del entry_yield, raw_crop_area, raw_crop_production, raw_crop_area_tot

    cleaned_crop_data = pd.DataFrame(cleaned_crop_data)

    return cleaned_crop_data


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1900,1901,1902,...
        Crop yield for each category[Africa,Pulses],x,x,x,...
        Crop yield for each category[AsiaPacific,Pulses],x,x,x,...
        ...
        Crop yield for each category[WestEu,OtherCrops],x,x,x,...
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

    food_categories = ["Pulses", "Grains", "VegFruits", "OtherCrops"]

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "year", "food_category"])
    structured_data = []
    for region in regions:
        for food_category in food_categories:
            entry_yield = {
                "parameter": f"Crop yield for each category[{region},{food_category}]",
            }
            entry_harvested_area = {
                "parameter": f"Area harvested[{region},{food_category}]",
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_crop_data_ = clean_data_groups.get_group(
                            (region, year, food_category)
                        )

                        entry_yield[year] = cleaned_crop_data_["yield"].values[0]
                        entry_harvested_area[year] = cleaned_crop_data_[
                            "harvested_area"
                        ].values[0]
                    except KeyError:
                        entry_yield[year] = np.nan
                        entry_harvested_area[year] = np.nan
                else:
                    entry_yield[year] = np.nan
                    entry_harvested_area[year] = np.nan

                del year

            structured_data.append(entry_yield)
            structured_data.append(entry_harvested_area)
            del entry_yield

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_crop_data = data_cleaning(raw_crop_data, concordance_table)

logging.info(f"Start restructuring the cleaned data")
restructured_crop_data = data_restructure(cleaned_crop_data)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_crop_data.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
