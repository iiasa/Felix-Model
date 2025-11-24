# -*- coding: utf-8 -*-
"""
Created: Tue 20 Feburary 2024
Description: Scripts to process mortality data by region from UNPD to 5 FeliX regions
Scope: FeliX model regionalization, module mortality 
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

# predefine the data source of mortality
# data sources are wittgensteinwittgenstein, world_bank, unpd
variable = "mortality"
data_source = "unpd"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/mortality/config.yaml")
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
download_method = raw_data_info["download_via"]
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
genders = data_info["dimension"]["gender"]
age_cohorts = data_info["dimension"]["age"]
logging.info("Extracted dimensions of regions, genders, and ages")

# Read raw data
logging.info(f"Read raw data")
raw_mortality = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        raw_mortality_ = pd.read_csv(path_raw_data_folder / raw_data_file)

        raw_mortality = pd.concat(
            [raw_mortality, raw_mortality_],
            ignore_index=True,
        )
        del raw_mortality_

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_mortality_dep = pd.DataFrame()
    if raw_data_info["datasource"] == "wittgenstein":
        logging.info("1 data source is wittgenstein")
    elif raw_data_info["datasource"] == "world_bank":
        logging.info("2 data source is world_bank")
    elif raw_data_info["datasource"] == "unpd":
        for raw_data_file_dep in raw_data_files_dep:
            logging.info(f"Data input: {path_raw_data_folder_dep / raw_data_file_dep}")
            raw_mortality_dep_ = pd.read_csv(
                path_raw_data_folder_dep / raw_data_file_dep
            )
            raw_mortality_dep = pd.concat(
                [raw_mortality_dep, raw_mortality_dep_],
                ignore_index=True,
            )
            del raw_mortality_dep_

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
            left_on="Location",
            right_on="region_api",
        ).rename(
            columns={
                "Location": "location",
                "TimeLabel": "year",
                "AgeLabel": "age",
                "Sex": "sex",
                "Value": "mortality",
            }
        )
    else:
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["region", "un_region"]],
            left_on="Location",
            right_on="region",
        ).rename(
            columns={
                "Location": "location",
                "TimeLabel": "year",
                "AgeLabel": "age",
                "Sex": "sex",
                "Value": "mortality",
            }
        )

    raw_data_merge = raw_data_merge[
        [
            "location",
            "year",
            "age",
            "sex",
            "mortality",
            "region_api",
            "un_region",
        ]
    ]

    if kwargs:
        if download_method == "api":
            raw_data_merge_dep = pd.merge(
                kwargs["kwargs"]["raw_data_dep"],
                concordance[["region_api", "un_region"]],
                left_on="Location",
                right_on="region_api",
            ).rename(
                columns={
                    "Location": "location",
                    "TimeLabel": "year",
                    "AgeLabel": "age",
                    "Sex": "sex",
                    "Value": "population",
                }
            )
        else:
            raw_data_merge_dep = pd.merge(
                kwargs["kwargs"]["raw_data_dep"],
                concordance[["region", "un_region"]],
                left_on="Location",
                right_on="region",
            ).rename(
                columns={
                    "Location": "location",
                    "TimeLabel": "year",
                    "AgeLabel": "age",
                    "Sex": "sex",
                    "Value": "population",
                }
            )
        raw_data_merge_dep = raw_data_merge_dep[
            [
                "location",
                "year",
                "age",
                "sex",
                "population",
                "region_api",
                "un_region",
            ]
        ]

        years = np.unique(raw_data_merge["year"])
        raw_data_groups = raw_data_merge.groupby(
            [
                "un_region",
                "year",
                "sex",
                "age",
            ]
        )
        raw_data_groups_dep = raw_data_merge_dep.groupby(
            [
                "un_region",
                "year",
                "sex",
                "age",
            ]
        )
        cleaned_data = []
        for region in regions:
            for year in years:
                for sex in genders:
                    for age in age_cohorts:
                        age = age.replace("--", "-")
                        if age == "0-4":
                            raw_data_region_year = raw_data_groups.get_group(
                                (
                                    region,
                                    year,
                                    sex.capitalize(),
                                    "1-4",
                                )
                            )
                            raw_data_region_year["age"] = "0-4"
                        else:
                            raw_data_region_year = raw_data_groups.get_group(
                                (
                                    region,
                                    year,
                                    sex.capitalize(),
                                    age,
                                )
                            )

                        if region != "World":
                            raw_data_region_year_dep = raw_data_groups_dep.get_group(
                                (
                                    region,
                                    year,
                                    sex.capitalize(),
                                    age,
                                )
                            )
                            raw_data_region_year_merge = pd.merge(
                                raw_data_region_year,
                                raw_data_region_year_dep[["location", "population"]],
                                on="location",
                            ).dropna()
                            del raw_data_region_year, raw_data_region_year_dep

                            mortality_avg = (
                                raw_data_region_year_merge["mortality"]
                                * raw_data_region_year_merge["population"]
                            ).sum() / raw_data_region_year_merge["population"].sum()
                        else:
                            mortality_avg = raw_data_region_year["mortality"].values[0]

                        entry = {
                            "un_region": region,
                            "sex": sex,
                            "age": age,
                            "year": year,
                            "value": mortality_avg,
                            "unit": "person",
                        }

                        cleaned_data.append(entry)
                        del entry, mortality_avg

        cleaned_data = pd.DataFrame(cleaned_data)
        cleaned_data = cleaned_data.astype({"year": "int"})

        return cleaned_data
    else:
        raise KeyError("No dependent data given")


# Define data restructure function
def data_restructure(
    cleaned_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
      '''
          Time,1950,1951,1952,...
          Mortality fraction[Africa],x,x,x,...
          Mortality fraction[AsiaPacific],x,x,x,...
          ......
          Mortality fraction[WestEU],x,x,x,...
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
    # cleaned_data = cleaned_data.astype({"year": "int"})
    year_avail = np.unique(cleaned_data["year"])

    restructured_data = []
    cleaned_data_by_region = cleaned_data.groupby(["un_region", "sex", "age", "year"])
    for region in regions:
        for sex in genders:
            for age in age_cohorts:
                age = age.replace("--", "-")
                entry = {"parameter": f"""Mortality fraction[{region},{sex},"{age}"]"""}
                for year in range(1900, 2101):
                    if year in year_avail:
                        data_by_age = cleaned_data_by_region.get_group(
                            (
                                region,
                                sex,
                                age,
                                year,
                            )
                        )["value"].values[0]
                        entry[year] = data_by_age
                        del data_by_age
                    else:
                        entry[year] = ""
                restructured_data.append(entry)
                del entry
    del cleaned_data_by_region

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_mortality = data_cleaning(
    raw_data=raw_mortality,
    raw_data_source=data_source,
    concordance=concordance_table,
    raw_data_dep=raw_mortality_dep,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_mortality = data_restructure(
    cleaned_data=cleaned_mortality,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_mortality.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_by_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
