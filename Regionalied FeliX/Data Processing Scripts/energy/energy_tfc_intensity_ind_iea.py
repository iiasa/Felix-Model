# -*- coding: utf-8 -*-
"""
Created: Fri 27 September 2024
Description: Scripts to calculate final energy consumption intensity of industry of FeliX regions
Scope: FeliX model regionalization, module energy 
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
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()


data_source = "iea"
variable = "tfc_int_ind"
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

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path(raw_data_info["root_path"]).joinpath(
    f"version_{version}/concordance"
)
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Check the dependency to clean raw data")
raw_data_dependency = raw_data_info["dependency"]
if raw_data_dependency:
    felix_module_dep = raw_data_info["dependency_module"]
    raw_data_files_dep = raw_data_info["dependency_file"]
    path_raw_data_folder_dep = Path(raw_data_info["root_path"]).joinpath(
        f"version_{version}/{felix_module_dep}"
    )
    concordance_file_dep = raw_data_info["dependency_concordance"]
logging.info("Check the dependency done")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")


logging.info("Read raw final energy consumption data")
raw_fact = pd.DataFrame()
for path_region in (path_raw_data_folder / "Industry Sector").glob("*"):
    if "Africa" in str(path_region):
        region = "Africa"
    elif "Pacific" in str(path_region):
        region = "AsiaPacific"
    elif "East" in str(path_region):
        region = "EastEu"
    elif "LAC" in str(path_region):
        region = "LAC"
    elif "West" in str(path_region):
        region = "WestEu"

    for path_fact in path_region.glob("*"):
        location = str(path_fact).split("- ")[-1][:-4]
        logging.info(f"Read raw data of {location} in {region}")
        raw_fact_location = pd.read_csv(
            path_fact,
            skiprows=3,
            encoding="utf-8",
        ).rename(
            columns={
                "Unnamed: 0": "year",
            }
        )
        raw_fact_location["tfc"] = raw_fact_location[
            [
                column_name
                for column_name in raw_fact_location.columns
                if column_name not in ["year", "Units"]
            ]
        ].sum(axis=1, skipna=True)
        raw_fact_location["region"] = region
        raw_fact_location["location"] = location

        raw_fact = pd.concat(
            [raw_fact, raw_fact_location],
            ignore_index=True,
        )
logging.info(f"Finish reading raw data")

logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_fact_dep = pd.DataFrame()
    if raw_data_info["dependency_datasource"] == "wittgenstein":
        logging.info("1 data source is unpd")
    elif raw_data_info["dependency_datasource"] == "world_bank":
        for raw_data_file_dep in raw_data_files_dep:
            logging.info(
                f"Dependent data input: {path_raw_data_folder_dep / raw_data_file_dep}"
            )
            with open(path_raw_data_folder_dep / raw_data_file_dep) as fact_file_dep:
                raw_fact_dep_ = json.load(fact_file_dep)

            for data_point_dep in raw_fact_dep_:
                if data_point_dep["value"] == "":
                    data_point_dep["value"] = np.nan
                    del data_point_dep

            raw_fact_dep_ = pd.DataFrame(raw_fact_dep_)
            raw_fact_dep = pd.concat(
                [raw_fact_dep, raw_fact_dep_],
                ignore_index=True,
            )
            del raw_fact_dep_

    elif raw_data_info["dependency_datasource"] == "unpd":
        logging.info("3 data source is unpd")


logging.info("Start reading dependent condordance table")
concordance_table_dep = pd.read_csv(
    path_concordance_folder / concordance_file_dep,
    encoding="utf-8",
)
concordance_table_dep = concordance_table_dep.dropna()
logging.info(f"Finish reading concordance table")


# Define cleaning function
def data_cleaning(
    raw_data: pd.DataFrame,
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

    if kwargs:
        raw_data_merge_dep = pd.merge(
            kwargs["raw_data_dep"],
            concordance_table_dep[["country_iea", "location_world_bank"]],
            left_on="country",
            right_on="location_world_bank",
        ).rename(
            columns={
                "time": "year",
                "value": "value_dep",
            }
        )

    years = np.unique(raw_data["year"])  # omit 2022
    raw_data_groups = raw_data.groupby(["region", "year"])
    raw_data_groups_dep = raw_data_merge_dep.groupby("year")
    cleaned_fact = []
    raw_data_region_test = pd.DataFrame()
    for region in regions:
        for year in years:
            try:
                raw_data_region = raw_data_groups.get_group((region, year))
            except KeyError:
                continue

            try:
                raw_data_region_dep = raw_data_groups_dep.get_group(str(year))
            except KeyError:
                continue

            raw_data_region_merge = pd.merge(
                raw_data_region,
                raw_data_region_dep[["value_dep", "country_iea"]],
                left_on="location",
                right_on="country_iea",
            )
            raw_data_region_merge = raw_data_region_merge.dropna(
                subset=["tfc", "value_dep"]
            )
            raw_data_region_test = pd.concat(
                [raw_data_region_test, raw_data_region_merge],
                ignore_index=True,
            )

            entry = {
                "region": region,
                "year": year,
                "unit": "TJ per 2015 $",
                "value": raw_data_region_merge["tfc"].sum()
                / raw_data_region_merge["value_dep"].sum(),
            }

            cleaned_fact.append(entry)
            del entry, raw_data_region, raw_data_region_merge

    cleaned_fact = pd.DataFrame(cleaned_fact)
    raw_data_region_test.to_csv("raw_data_region_test.csv")
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
        TFC Intensity Industry[Africa],x,x,x,...
        ...
        TFC Intensity Industry[WestEu],x,x,x,...
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
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "year"])
    structured_data = []
    for region in regions:
        entry = {
            "parameter (unit: TJ per 2015 $)": f"TFC Intensity Industry[{region}]",
        }
        for year in range(1900, 2101):
            if year in years:
                try:
                    cleaned_fact = clean_data_groups.get_group((region, year))
                    entry[year] = cleaned_fact["value"].values[0]
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
logging.info("Start script to aggregate final energy consumption data from IEA")

logging.info(f"Start cleaning the raw data")
cleaned_fact = data_cleaning(
    raw_data=raw_fact,
    raw_data_dep=raw_fact_dep,
)
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
