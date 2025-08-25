# -*- coding: utf-8 -*-
"""
Created: Friday 08 November 2024
Description: Scripts to aggregate energy supply data by source and region from IEA to FeliX regions
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
data_source = "iea"
variable = "energy_supply"
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
    f"version_{version}/{felix_module}/{variable}"
)
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
raw_energy_sup = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("3 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "irena":
    logging.info("4 data source is irena")
elif raw_data_info["datasource"] == "iea":
    for raw_data_file in path_raw_data_folder.glob("*"):
        location = str(raw_data_file).split("- ")[-1][:-4]
        logging.info(f"Read raw data of {location}")
        raw_energy_sup_location = pd.read_csv(
            raw_data_file,
            skiprows=3,
            encoding="utf-8",
        ).rename(
            columns={
                "Unnamed: 0": "year",
            }
        )

        raw_energy_sup_location["country"] = location

        raw_energy_sup = pd.concat(
            [raw_energy_sup, raw_energy_sup_location],
            ignore_index=True,
        )
        del raw_energy_sup_location

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_energy_sup_dep = pd.DataFrame()
    if raw_data_info["datasource"] == "wittgenstein":
        logging.info("1 data source is wittgenstein")
    elif raw_data_info["datasource"] == "world_bank":
        logging.info("1 data source is world_bank")
    elif raw_data_info["datasource"] == "unpd":
        logging.info("3 data source is unpd")
    elif raw_data_info["datasource"] == "irena":
        logging.info("3 data source is irena")
    elif raw_data_info["datasource"] == "iea":
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
    if download_method == "manually":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", "un_region"]],
            on="country",
        )
    else:
        logging.warning(f"Download method is not manually")
        raise KeyError
    raw_data_merge.to_csv("test_raw_data_merge.csv")

    raw_data_merge.columns = raw_data_merge.columns.str.lower()
    logging.info("Specify energy sources")
    energy_sources = []
    for column_name in raw_data_merge.columns:
        if column_name not in ["year", "units", "un_region", "country"]:
            energy_sources.append(column_name)
    logging.info("Specify available years")
    years = np.unique(raw_data_merge["year"])

    raw_data_merge_groups = raw_data_merge.groupby(["un_region", "year"])
    cleaned_energy_sup = []
    for region in regions:
        for year in years:
            try:
                raw_data_region = raw_data_merge_groups.get_group((region, year))
            except KeyError:
                continue

            for energy_source in energy_sources:
                entry = {
                    "region": region,
                    "energy_source": energy_source,
                    "year": year,
                    "unit": "TJ",
                    "value": raw_data_region[energy_source].sum(),
                }

                cleaned_energy_sup.append(entry)
                del entry, energy_source
            del raw_data_region

    cleaned_energy_sup = pd.DataFrame(cleaned_energy_sup)
    return cleaned_energy_sup


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the cleaning function into the format:
    '''
        Parameter,1950,1951,1952,...
        Total Energy Supply by source[Africa,energy source 1],x,x,x,...
        Total Energy Supply by source[Africa,energy source 1],x,x,x,...
        ...
        Total Energy Supply by source[WestEu,energy source n-1],x,x,x,...
        Total Energy Supply by source[WestEu,energy source n],x,x,x,...
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
                "parameter (unit: TJ)": f"Total Energy Supply by source",
                "region": region,
                "energy_source": energy_source,
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_energy_sup = clean_data_groups.get_group(
                            (region, energy_source, year)
                        )
                        entry[year] = cleaned_energy_sup["value"].values[0]
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
cleaned_energy_sup = data_cleaning(raw_energy_sup, concordance_table)
logging.info("Finish data cleaning")

logging.info(f"Start restructuring the cleaned data")
restructured_energy_sup = data_restructure(cleaned_energy_sup)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_energy_sup.to_csv(
    path_clean_data_folder.joinpath(
        f"{variable}_by_source_time_series_{data_source}.csv"
    ),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
