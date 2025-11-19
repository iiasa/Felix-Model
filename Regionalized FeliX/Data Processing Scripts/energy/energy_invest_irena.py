# -*- coding: utf-8 -*-
"""
Created: Wed 30 October 2024
Description: Scripts to aggregate energy investment by source and region from IRENA to FeliX regions
Scope: FeliX model regionalization, module energy 
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of energy
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "irena"
variable = "irena_invest"
download_method = "manually"

# read config.yaml file
yaml_dir = Path("scripts/energy/config.yaml")
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

logging.info("Check the dependency to clean raw data")
raw_data_dependency = raw_data_info["dependency"]
if raw_data_dependency:
    felix_module_dep = raw_data_info["dependency_module"]
    raw_data_files_dep = raw_data_info["dependency_file"]
    path_raw_data_folder_dep = Path(raw_data_info["root_path"]).joinpath(
        f"version_{version}/{felix_module_dep}"
    )

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
raw_invest = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("3 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "irena":
    for raw_data_file in raw_data_files:
        if download_method == "manually":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            raw_invest_ = pd.read_excel(
                path_raw_data_folder / raw_data_file, sheet_name="July2022"
            )

            raw_invest = pd.concat(
                [raw_invest, raw_invest_],
                ignore_index=True,
            )
            del raw_invest_
        else:
            logging.warning("The download method is not API")
            raise KeyError

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_invest_dep = pd.DataFrame()
    if raw_data_info["datasource"] == "wittgenstein":
        logging.info("1 data source is wittgenstein")
    elif raw_data_info["datasource"] == "world_bank":
        logging.info("1 data source is world_bank")
    elif raw_data_info["datasource"] == "unpd":
        logging.info("3 data source is unpd")


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

    concordance: pd.DataFrame
        Concordance table between countries and 5 UN regions

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    cleaned data in pd.Dataframe

    """
    raw_data.columns = raw_data.columns.str.lower()
    logging.info("Specify energy sources")
    energy_sources = np.unique(raw_data["technology"])

    logging.info("Specify available years")
    raw_data["year"] = pd.to_datetime(
        raw_data["reference date"], format="%d/%m/%Y"
    ).dt.year
    years = np.unique(raw_data["year"])

    logging.info("Merge cleaned data with regional concordance")
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["country", "un_region"]],
        left_on="country/area",
        right_on="country",
    )
    del raw_data

    raw_data_groups = raw_data_merge.groupby(["un_region", "technology", "year"])
    cleaned_fact = []
    for region in regions:
        for energy_source in energy_sources:
            for year in years:
                try:
                    raw_data_region = raw_data_groups.get_group(
                        (region, energy_source, year)
                    )
                except KeyError:
                    continue

                entry = {
                    "region": region,
                    "energy_source": energy_source,
                    "year": year,
                    "unit": "2020 USD",
                    "value": raw_data_region["amount (2020 usd million)"].sum()
                    * 1000000,
                }

                cleaned_fact.append(entry)
                del entry, raw_data_region

    cleaned_fact = pd.DataFrame(cleaned_fact)

    return cleaned_fact


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1950,1951,1952,...
        Investment 2020 USD[Africa,energy source 1],x,x,x,...
        Investment 2020 USD[Africa,energy source 2],x,x,x,...
        ...
        Investment 2020 USD[WestEu,energy source n-1],x,x,x,...
        Investment 2020 USD[WestEu,energy source n],x,x,x,...
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
    energy_sources = np.unique(clean_data["energy_source"])
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "energy_source", "year"])
    structured_data = []
    for region in regions:
        for energy_source in energy_sources:
            entry = {
                "parameter": f"Investment 2020 USD",
                "region": region,
                "energy_source": energy_source,
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_fact = clean_data_groups.get_group(
                            (region, energy_source, year)
                        )
                        entry[year] = cleaned_fact["value"].values[0]
                    except KeyError:
                        entry[year] = np.nan
                else:
                    entry[year] = np.nan

                del year

            structured_data.append(entry)
            del entry, energy_source

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_fact = data_cleaning(raw_data=raw_invest, concordance=concordance_table)
logging.info("Finish data cleaning")

logging.info(f"Start restructuring the cleaned data")
restructured_fact = data_restructure(cleaned_fact)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_fact.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
