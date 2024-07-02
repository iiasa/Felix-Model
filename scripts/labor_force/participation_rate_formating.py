# -*- coding: utf-8 -*-
"""
Created: Mon 03 June 2024
Description: Scripts to process participation rates from ILO to 5 FeliX regions
Scope: FeliX model regionalization, module labor_force 
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

# predefine the data source of labor_force
# data sources are wittgensteinwittgenstein, world_bank, unpd
variable = "participation_rate"
data_source = "ilo"
download_method = "manually"

# read config.yaml file
yaml_dir = Path("scripts/labor_force/config.yaml")
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
raw_labor_force = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "ilo":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        raw_labor_force_ = pd.read_csv(
            path_raw_data_folder / raw_data_file,
            encoding="utf-8",
        )

        raw_labor_force = pd.concat(
            [raw_labor_force, raw_labor_force_],
            ignore_index=True,
        )
        del raw_labor_force_
    raw_labor_force.columns = raw_labor_force.columns.str.lower()

    logging.info("Need to convert dimensions of fact from codes into texts")
    with open(
        path_raw_data_folder / "dimmension_codelists_ilo.yaml", "r"
    ) as dimension_file:
        dimensions = yaml.safe_load(dimension_file)

    for dimension in dimensions["variables"]:
        dim_code = dimension["code"]
        dim_name = dimension["text"]

        cleaned_dimension = []
        for pos, value_ in enumerate(dimension["values"]):
            cleaned_dimension.append(
                {"dim_id": value_, "dim_text": dimension["valueTexts"][pos]}
            )
            del pos
        cleaned_dimension = pd.DataFrame(cleaned_dimension)

        if "AREA" in dim_code:
            raw_labor_force = pd.merge(
                raw_labor_force,
                cleaned_dimension,
                left_on="ref_area",
                right_on="dim_id",
            ).rename(
                columns={
                    "dim_id": "ilo_location_id",
                    "dim_text": "ilo_location",
                }
            )

        elif "SEX" in dim_code:
            raw_labor_force = pd.merge(
                raw_labor_force,
                cleaned_dimension,
                left_on="sex",
                right_on="dim_id",
            ).rename(
                columns={
                    "dim_id": "sex_id",
                    "dim_text": "gender",
                }
            )
        else:
            raw_labor_force = pd.merge(
                raw_labor_force,
                cleaned_dimension,
                left_on="classif1",
                right_on="dim_id",
            ).rename(
                columns={
                    "dim_id": "age_id",
                    "dim_text": "age",
                }
            )

# Read raw dependency data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_labor_force_dep = pd.DataFrame()
    if raw_data_info["dependency_datasource"] == "wittgenstein":
        logging.info("1 data source is wittgenstein")
    elif raw_data_info["dependency_datasource"] == "world_bank":
        logging.info("2 data source is world_bank")
    elif raw_data_info["dependency_datasource"] == "unpd":
        for raw_data_file_dep in raw_data_files_dep:
            logging.info(f"Data input: {path_raw_data_folder_dep / raw_data_file_dep}")
            raw_labor_force_dep_ = pd.read_csv(
                path_raw_data_folder_dep / raw_data_file_dep
            )
            raw_labor_force_dep = pd.concat(
                [raw_labor_force_dep, raw_labor_force_dep_],
                ignore_index=True,
            )
            del raw_labor_force_dep_
raw_labor_force_dep.columns = raw_labor_force_dep.columns.str.lower()

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
    elif raw_data_source == "ilo":
        cleaned_data = data_cleaning_ilo(
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
    print("this is the data_cleaning_unpd")


def data_cleaning_ilo(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw labor force participation rate data from ILO to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from ILO

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
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["ilo_location", "unpd_region", "un_region"]],
        on="ilo_location",
    )
    logging.info("Select data that is requested")
    raw_data_merge = raw_data_merge[
        [
            "unpd_region",
            "gender",
            "age",
            "time",
            "obs_value",
            "un_region",
        ]
    ].rename(
        columns={
            "gender": "sex",
            "time": "year",
            "obs_value": "participation_rate",
        }
    )

    if kwargs:
        raw_data_merge_dep = pd.merge(
            kwargs["kwargs"]["raw_data_dep"],
            concordance[["unpd_region", "un_region"]],
            left_on="location",
            right_on="unpd_region",
        ).rename(
            columns={
                "location": "location",
                "timelabel": "year",
                "agelabel": "age",
                "sex": "sex",
                "value": "population",
            }
        )

        raw_data_merge_dep = raw_data_merge_dep[
            [
                "unpd_region",
                "year",
                "age",
                "sex",
                "population",
                "un_region",
            ]
        ]
    else:
        raise KeyError("No dependent data given")

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(
        [
            "un_region",
            "sex",
            "age",
            "year",
        ]
    )
    raw_data_groups_dep = raw_data_merge_dep.groupby(
        [
            "un_region",
            "sex",
            "age",
            "year",
        ]
    )
    cleaned_data = []
    for sex in genders:
        sex = sex.capitalize()
        for age in age_cohorts:
            for year in years:
                cleaned_data_ = []  # will be used to calculate global average
                for region in regions:
                    if region != "World":
                        try:
                            raw_data_region_year = raw_data_groups.get_group(
                                (
                                    region,
                                    sex,
                                    age,
                                    year,
                                )
                            )
                        except KeyError:
                            continue

                        try:
                            raw_data_region_year_dep = raw_data_groups_dep.get_group(
                                (
                                    region,
                                    sex,
                                    age,
                                    int(year),
                                )
                            )
                        except KeyError:
                            continue

                        raw_data_region_year_merge = pd.merge(
                            raw_data_region_year,
                            raw_data_region_year_dep[["unpd_region", "population"]],
                            on="unpd_region",
                        ).dropna()
                        del raw_data_region_year, raw_data_region_year_dep

                        participation_rate_avg = (
                            raw_data_region_year_merge["participation_rate"]
                            * raw_data_region_year_merge["population"]
                        ).sum() / raw_data_region_year_merge["population"].sum()

                        entry = {
                            "un_region": region,
                            "sex": sex.lower(),
                            "age": age,
                            "year": year,
                            "value": participation_rate_avg / 100,
                            "population": raw_data_region_year_merge[
                                "population"
                            ].sum(),
                        }

                        cleaned_data.append(entry)
                        cleaned_data_.append(entry)
                        del raw_data_region_year_merge, entry

                if len(cleaned_data_) == 5:
                    cleaned_data_ = pd.DataFrame(cleaned_data_)

                    participation_rate_avg = (
                        cleaned_data_["value"] * cleaned_data_["population"]
                    ).sum() / cleaned_data_["population"].sum()
                    entry = {
                        "un_region": "World",
                        "sex": sex.lower(),
                        "age": age,
                        "year": year,
                        "value": participation_rate_avg,
                        "population": cleaned_data_["population"].sum(),
                    }

                    cleaned_data.append(entry)
                    del cleaned_data_, entry

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
          Participation Fraction[Africa，male,'15-19'],x,x,x,...
          Participation Fraction[Africa，male,'20-24'],x,x,x,...
          ......
          Participation Fraction[WestEU,female,'60-64'],x,x,x,...
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
    logging.info("Restructure cleaned data by year")
    cleaned_data_by_region = cleaned_data.groupby(["un_region", "sex", "age", "year"])
    for region in regions:
        for sex in genders:
            for age in age_cohorts:
                entry = {
                    "parameter": f"""Participation Fraction[{region},{sex},"{age}"]"""
                }
                for year in range(1900, 2101):
                    if year in year_avail:
                        try:
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
                        except KeyError:
                            entry[year] = ""
                            continue
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
cleaned_labor_force = data_cleaning(
    raw_data=raw_labor_force,
    raw_data_source=data_source,
    concordance=concordance_table,
    raw_data_dep=raw_labor_force_dep,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_labor_force = data_restructure(
    cleaned_data=cleaned_labor_force,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_labor_force.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_by_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
