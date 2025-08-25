# -*- coding: utf-8 -*-
"""
Created: Thur 14 Feburary 2024
Description: Scripts to process total death rate data by region from UNPD to 5 FeliX regions
Scope: FeliX model regionalization, module death_rate 
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
import pyam
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of death_rate
# data sources are wittgensteinwittgenstein, world_bank, unpd
variable = "total_death"
data_source = "unpd"

# read config.yaml file
yaml_dir = Path("scripts/death_rate/config.yaml")
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
    if (dataset["datasource"] == data_source) and (dataset["variable"] == variable):
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
download_method = raw_data_info["download_via"]
logging.info("Path to raw data set")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path(raw_data_info["root_path"]).joinpath(
    f"version_{version}/concordance"
)
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
genders = data_info["dimension"]["gender"]
logging.info("Extracted dimensions of regions, genders, and ages")

# Read raw data
logging.info(f"Read raw data")
raw_death_rate = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        with open(path_raw_data_folder / raw_data_file) as fact_file:
            raw_death_rate_ = json.load(fact_file)
        raw_death_rate_ = pd.DataFrame(raw_death_rate_)

        raw_death_rate_ = raw_death_rate_.loc[
            raw_death_rate_["variant"] == "Median"
        ].reset_index(drop=True)
        raw_death_rate = pd.concat(
            [raw_death_rate, raw_death_rate_],
            ignore_index=True,
        )
        del raw_death_rate_

logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


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
    print("this is the data_cleaning_world_bank")


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
    if download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["region_api", "un_region"]],
            left_on="location",
            right_on="region_api",
        )
    else:
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["region", "un_region"]],
            left_on="location",
            right_on="region",
        )

    raw_data_merge = raw_data_merge[
        [
            "location",
            "sex",
            "timeLabel",
            "value",
            "un_region",
        ]
    ].rename(
        columns={
            "timeLabel": "year",
        }
    )

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(["un_region", "sex", "year"])
    cleaned_data = []
    for region in regions:
        for sex in genders:
            for year in years:
                raw_data_region_year = raw_data_groups.get_group(
                    (
                        region,
                        sex.capitalize(),
                        year,
                    )
                )

                entry = {
                    "un_region": region,
                    "sex": sex,
                    "year": year,
                    "value": raw_data_region_year["value"].sum(),
                    "unit": "person",
                }

                cleaned_data.append(entry)

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


# Define data restructure function
def data_restructure(
    cleaned_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
      '''
          Time,1950,1951,1952,...
          Total Death Rate[Africa],x,x,x,...
          Total Death Rate[AsiaPacific],x,x,x,...
          ......
          Total Death Rate[WestEU],x,x,x,...
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
    logging.info("Sum up cleaned data by gender")
    cleaned_data_by_region = cleaned_data.groupby(["un_region", "year"])["value"].sum()
    for region in regions:
        entry = {"parameter": f"Total Death Rate[{region}]"}
        for year in range(1900, 2101):
            if year in year_avail:
                data_by_age = cleaned_data_by_region.loc[
                    region,
                    year,
                ]
                entry[year] = data_by_age
                del data_by_age
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
cleaned_death_rate = data_cleaning(
    raw_data=raw_death_rate,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_death_rate = data_restructure(cleaned_data=cleaned_death_rate)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_death_rate.to_csv(
    path_clean_data_folder.joinpath(f"death_rate_by_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
