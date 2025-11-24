# -*- coding: utf-8 -*-
"""
Created: Wed 28 Feburary 2024
Description: Scripts to calculate net enrollment of educational levels (%) by region from World Bank to 5 FeliX regions
Scope: FeliX model regionalization, module education 
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

# predefine the data source of education
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "world_bank"
variable = "gross_enroll_tertiary"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/education/config.yaml")
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
variable_text = raw_data_info["text"]
unit = raw_data_info["unit"]

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
genders = data_info["dimension"]["gender"]
logging.info("Extracted dimensions of regions and genders")

# Read raw data
logging.info(f"Read raw data")
raw_education = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "api":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            with open(path_raw_data_folder / raw_data_file) as fact_file:
                raw_education_ = json.load(fact_file)
            for pos, data_point in enumerate(raw_education_):
                if data_point["value"] == "":
                    raw_education_[pos]["value"] = np.nan
                    del data_point, pos

            logging.info("Need to set the gender dimension")
            if "female" in raw_education_[0]["series"].lower():
                sex = "female"
            else:
                sex = "male"
            raw_education_ = pd.DataFrame(raw_education_)
            raw_education_["sex"] = sex
            raw_education = pd.concat(
                [raw_education, raw_education_],
                ignore_index=True,
            )
            del raw_education_
        else:
            logging.warning("The download method is not API")
            raise KeyError
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
    """
    To clean raw data from World Bank to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from World Bank

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
    logging.info("Merge cleaned data with regional concordance")
    if download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["location", "un_region"]],
            left_on="country",
            right_on="location",
        ).rename(
            columns={
                "time": "year",
                "value": variable,
            }
        )
    else:
        logging.warning(f"Download method is not api")
        raise KeyError

    years = np.unique(raw_data_merge["year"])
    logging.info(
        "Group cleaned data and cleaned dependent data based on certain dimensions"
    )
    raw_data_groups = raw_data_merge.groupby(["un_region", "sex", "year"])
    cleaned_data = []
    for year in years:
        for sex in genders:
            # extract the global average enrollment (%)
            raw_global = raw_data_groups.get_group(
                (
                    "World",
                    sex,
                    year,
                )
            )[
                variable
            ].values[0]

            if not np.isnan(raw_global):
                for region in regions:
                    # extract regional net enrollment  (%)
                    raw_data_year = raw_data_groups.get_group(
                        (
                            region,
                            sex,
                            year,
                        )
                    )

                    raw_data_year.dropna(
                        subset=[variable],
                        inplace=True,
                    )

                    if raw_data_year.empty is False:
                        # calculate the median values of available net enrollment as the regional enrollment
                        enroll_median = raw_data_year[variable].median() / 100

                        entry = {
                            "un_region": region,
                            "sex": sex,
                            "year": year,
                            variable: enroll_median,
                            "whether_world_average": raw_data_year.empty,
                        }
                        del enroll_median
                    else:
                        entry = {
                            "un_region": region,
                            "sex": sex,
                            "year": year,
                            variable: raw_global / 100,
                            "whether_world_average": raw_data_year.empty,
                        }

                    cleaned_data.append(entry)
                    del entry
                del raw_global

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


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


# Define data restructure function
def data_restructure(
    cleaned_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
      '''
          Time,1950,1951,1952,...
          Net enrollment <educational level>[Africa,female],x,x,x,...
          Net enrollment <educational level>[Africa,male],x,x,x,...
          ......
          Net enrollment <educational level>[WestEu,female],x,x,x,...
          Net enrollment <educational level>[WestEu,male],x,x,x,x,...
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
    logging.info("Group cleaned data by region, gender, and year")
    # cleaned_data = cleaned_data.astype({"year": "int"})
    year_avail = np.unique(cleaned_data["year"])

    restructured_data = []
    logging.info("Group cleaned data by all dimensions")
    cleaned_data_groups = cleaned_data.groupby(["un_region", "sex", "year"])
    for region in regions:
        for sex in genders:
            entry = {
                "parameter": f"{variable_text}[{region},{sex}]",
            }
            for year in range(1900, 2101):
                if year in year_avail:
                    data_by_all = cleaned_data_groups.get_group(
                        (
                            region,
                            sex,
                            year,
                        )
                    )

                    entry[year] = data_by_all[variable].values[0]

                    del data_by_all
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
cleaned_education = data_cleaning(
    raw_data=raw_education,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")

restructured_education = data_restructure(
    cleaned_data=cleaned_education,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_education.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_by_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
