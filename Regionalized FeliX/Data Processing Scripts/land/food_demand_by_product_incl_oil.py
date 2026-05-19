# -*- coding: utf-8 -*-
"""
Created: Sat 14 March 2026
Description: Scripts to aggregate food demand by product (incl. secondary oil product) from FAOSTAT to FeliX regions
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
from io import StringIO

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
data_variable = "food_demand_incl_oil"
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
food_categories = data_info["dimension"]["food_category"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_food_demand = pd.DataFrame()
for raw_data_file in (path_data_raw / raw_data_info["data_file"]).glob("*.csv"):
    logging.info(f"Read raw data of {raw_data_file}")
    raw_food_demand_ = pd.read_csv(raw_data_file, encoding="latin1").rename(
        columns={"Area": "country"}
    )

    raw_food_demand = pd.concat(
        [raw_food_demand, raw_food_demand_],
        ignore_index=True,
    )
    del raw_food_demand_

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
    To transfer raw data from FAOSTA database into FeliX region classification

    Parameter
    ---------
    raw_data: pd.DataFrame
        Data downloaded directly from FAOSTA database.

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
        if crop_name in ["Bovine Meat", "Mutton & Goat Meat"]:
            return "PasMeat"
        elif crop_name in ["Pigmeat", "Poultry Meat", "Fish, Seafood"]:
            return "CropMeat"
        elif crop_name in ["Milk - Excluding Butter", "Animal fats"]:
            return "Dairy"
        elif crop_name in ["Eggs"]:
            return "Eggs"
        elif crop_name in ["Pulses"]:
            return "Pulses"
        elif crop_name in ["Cereals - Excluding Beer"]:
            return "Grains"
        elif crop_name in ["Fruits - Excluding Wine", "Starchy Roots", "Vegetables"]:
            return "VegFruits"
        elif crop_name in [
            "Sugar & Sweeteners",
            "Treenuts",
            "Vegetable Oils",
        ]:
            return "OtherCrops"
        elif crop_name in ["Population"]:
            return "Population"

    raw_data_merge["food_category"] = (
        raw_data_merge["item"].astype(str).map(name_mapping)
    )

    raw_data_merge = (
        raw_data_merge[
            [
                "country",
                "element",
                "item",
                "year",
                "value",
                "un_region",
                "food_category",
            ]
        ]
        .pivot_table(
            index=["un_region", "country", "food_category", "year", "item"],
            columns="element",
            values="value",
        )
        .reset_index()
    )

    raw_data_merge_groups = raw_data_merge.groupby(
        ["un_region", "year", "food_category"]
    )

    cleaned_food_demand = []
    for region in regions:
        num_country_ = num_country[region]
        for year in years:
            for food_category in food_categories:
                try:
                    raw_data_region_year_ = raw_data_merge_groups.get_group(
                        (region, year, food_category)
                    )
                except KeyError:
                    continue
                raw_data_region_year_pop_ = raw_data_merge_groups.get_group(
                    (region, year, "Population")
                )

                if (
                    len(np.unique(raw_data_region_year_["country"])) / num_country_
                    > 0.7
                ):
                    if int(year) < 2010:
                        cal_content = (
                            raw_data_region_year_["Food supply (kcal/capita/day)"]
                            * 365
                            / raw_data_region_year_[
                                "Food supply quantity (kg/capita/yr)"
                            ]
                        )  # unit in kcal per kg

                        entry = {
                            "region": region,
                            "year": year,
                            "food_category": food_category,
                            "food_demand_tonnes": raw_data_region_year_["Food"].sum()
                            * 1000,  # unit in tonnes
                            "food_demand_Mkcal": sum(
                                raw_data_region_year_["Food"] * cal_content
                            ),  # unit in million kcal
                        }
                        del cal_content

                    else:

                        region_year_pop_ = (
                            raw_data_region_year_pop_[
                                raw_data_region_year_pop_["country"].isin(
                                    list(raw_data_region_year_["country"].unique())
                                )
                            ]
                            .iloc[:, -1]
                            .sum()
                            * 1000  # unit in person
                        )

                        entry = {
                            "region": region,
                            "year": year,
                            "food_category": food_category,
                            "food_demand_tonnes": raw_data_region_year_["Food"].sum()
                            * 1000,  # unit in tonnes
                            "food_demand_Mkcal": raw_data_region_year_[
                                "Food supply (kcal)"
                            ].sum(),  # unit in million kcal
                            "daily_food_demand_per_capita_kcal": raw_data_region_year_[
                                "Food supply (kcal)"
                            ].sum()
                            * 1000000
                            / region_year_pop_
                            / 365,
                        }

                    cleaned_food_demand.append(entry)
                    del entry, raw_data_region_year_

    cleaned_food_demand = pd.DataFrame(cleaned_food_demand)

    return cleaned_food_demand


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1900,1901,1902,...
        Annual Caloric Demand by Region[Africa,PasMeat],x,x,x,...
        Annual Caloric Demand by Region[AsiaPacific,PasMeat],x,x,x,...
        ...
        Annual Caloric Demand by Region[WestEu,OtherCrops],x,x,x,...
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
    clean_data_groups = clean_data.groupby(["region", "year", "food_category"])
    structured_data = []
    for region in regions:
        for food_category in food_categories:
            entry_kcal = {
                "parameter": f"Annual Caloric Demand by Region[{region},{food_category}]",
            }
            entry_tonnes = {
                "parameter": f"Food Demand excl Waste in tonnes[{region},{food_category}]",
            }
            entry_daily_per_capita = {
                "parameter": f"Average Kcal Intake per Person[{region},{food_category}]",
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_food_demand_ = clean_data_groups.get_group(
                            (region, year, food_category)
                        )

                        entry_kcal[year] = cleaned_food_demand_[
                            "food_demand_Mkcal"
                        ].values[0]
                        entry_tonnes[year] = cleaned_food_demand_[
                            "food_demand_tonnes"
                        ].values[0]
                        entry_daily_per_capita[year] = cleaned_food_demand_[
                            "daily_food_demand_per_capita_kcal"
                        ].values[0]
                    except KeyError:
                        entry_kcal[year] = np.nan
                        entry_tonnes[year] = np.nan
                        entry_daily_per_capita[year] = np.nan
                else:
                    entry_kcal[year] = np.nan
                    entry_tonnes[year] = np.nan
                    entry_daily_per_capita[year] = np.nan

                del year

            structured_data.append(entry_kcal)
            structured_data.append(entry_tonnes)
            structured_data.append(entry_daily_per_capita)
            del entry_kcal, entry_tonnes, entry_daily_per_capita

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_food_demand = data_cleaning(raw_food_demand, concordance_table)

logging.info(f"Start restructuring the cleaned data")
restructured_food_demand = data_restructure(cleaned_food_demand)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_food_demand.to_csv(
    path_data_clean / f"{data_variable}_time_series_{data_source}.csv",
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
