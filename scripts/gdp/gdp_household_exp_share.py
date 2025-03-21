# -*- coding: utf-8 -*-
"""
Created: March 14 2025
Description: Scripts to calculated shares of hosuehold expenditure in GDP for 5 FeliX regions
Scope: FeliX model regionalization, module gdp 
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import datetime
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

import os
from dotenv import load_dotenv

load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_FELIX_REGIONALIZATION")

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# read config.yaml file
try:
    yaml_dir = Path("scripts/gdp/config.yaml")
    with open(yaml_dir, "r") as dimension_file:
        data_info = yaml.safe_load(dimension_file)
except FileNotFoundError:
    yaml_dir = Path("config.yaml")
    with open(yaml_dir, "r") as dimension_file:
        data_info = yaml.safe_load(dimension_file)

# predefine the data source of gdp
# data sources are wittgensteinwittgenstein, world_bank, unpd, ggdc (Groningen Growth and Development Center)
logging.info("Configure module")
current_project = "felix_regionalization"
felix_module = data_info["module"]
data_source = "world_bank"
variable = "gdp_hh_exp_share"
download_method = "api"

logging.info("Configure paths")
# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = (
    data_home / "raw_data" / current_project / current_version / felix_module
)
path_clean_data_folder = (
    data_home / "clean_data" / current_project / current_version / felix_module
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

logging.info("Set path to raw data")
raw_data_files = raw_data_info["data_file"]

logging.info("Check the dependency to clean raw data")
raw_data_dependency = raw_data_info["dependency"]
if raw_data_dependency:
    felix_module_dep = raw_data_info["dependency_module"]
    raw_data_files_dep = raw_data_info["dependency_file"]
    path_raw_data_folder_dep = path_raw_data_folder.parent / felix_module_dep
logging.info("Check the dependency done")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = path_raw_data_folder.parent / "concordance"
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_hh_exp_share = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "api":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            with open(path_raw_data_folder / raw_data_file) as fact_file:
                raw_hh_exp_share_ = json.load(fact_file)
            for data_point in raw_hh_exp_share_:
                if data_point["value"] == "":
                    data_point["value"] = np.nan
                    del data_point

            raw_hh_exp_share_ = pd.DataFrame(raw_hh_exp_share_)
            raw_hh_exp_share = pd.concat(
                [raw_hh_exp_share, raw_hh_exp_share_],
                ignore_index=True,
            )
            del raw_hh_exp_share_
        else:
            logging.warning("The download method is not API")
            raise KeyError
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "ggdc":
    logging.info("3 data source is ggdc")
logging.info(f"Finish reading raw data")

logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_hh_exp_share_dep = pd.DataFrame()
    if raw_data_info["datasource"] == "wittgenstein":
        logging.info("1 data source is unpd")
    elif raw_data_info["datasource"] == "world_bank":
        for raw_data_file_dep in raw_data_files_dep:
            logging.info(
                f"Dependent data input: {path_raw_data_folder_dep / raw_data_file_dep}"
            )
            if download_method == "api":
                logging.info(
                    f"Dependent data input: {path_raw_data_folder_dep / raw_data_file_dep}"
                )
                with open(
                    path_raw_data_folder_dep / raw_data_file_dep
                ) as fact_file_dep:
                    raw_hh_exp_share_dep_ = json.load(fact_file_dep)

                for data_point_dep in raw_hh_exp_share_dep_:
                    if data_point_dep["value"] == "":
                        data_point_dep["value"] = np.nan
                        del data_point_dep

                raw_hh_exp_share_dep_ = pd.DataFrame(raw_hh_exp_share_dep_)
                raw_hh_exp_share_dep = pd.concat(
                    [raw_hh_exp_share_dep, raw_hh_exp_share_dep_],
                    ignore_index=True,
                )
                del raw_hh_exp_share_dep_
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
    elif raw_data_source == "ggdc":
        cleaned_data = data_cleaning_ggdc(
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
    if download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["location", "un_region"]],
            left_on="country",
            right_on="location",
        ).rename(
            columns={
                "time": "year",
            }
        )
    else:
        raise KeyError("No data downloaded manually from World Bank")

    if kwargs:
        if download_method == "api":
            raw_data_merge_dep = pd.merge(
                kwargs["kwargs"]["raw_data_dep"],
                concordance[["location", "un_region"]],
                left_on="country",
                right_on="location",
            ).rename(
                columns={
                    "time": "year",
                    "value": "value_dep",
                }
            )
        else:
            raise KeyError("No data downloaded manually from World Bank")

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(["un_region", "year"])
    raw_data_groups_dep = raw_data_merge_dep.groupby(["un_region", "year"])
    cleaned_data = []
    for year in years:
        for region in regions:
            try:
                raw_data_region_year = raw_data_groups.get_group(
                    (
                        region,
                        year,
                    )
                )
            except KeyError:
                continue

            try:
                raw_data_region_year_dep = raw_data_groups_dep.get_group(
                    (
                        region,
                        year,
                    )
                )
            except KeyError:
                continue

            raw_data_region_year_merge = pd.merge(
                raw_data_region_year,
                raw_data_region_year_dep,
                on="location",
            )
            del raw_data_region_year, raw_data_region_year_dep

            raw_data_region_year_merge_nonana = raw_data_region_year_merge.dropna()
            hh_exp_share = (
                (
                    raw_data_region_year_merge_nonana["value_dep"]
                    * raw_data_region_year_merge_nonana["value"]
                ).sum()
                / raw_data_region_year_merge_nonana["value_dep"].sum()
                / 100
            )

            entry = {
                "un_region": region,
                "year": year,
                "gdp": raw_data_region_year_merge["value_dep"].sum(),
                "hh_exp": raw_data_region_year_merge["value_dep"].sum() * hh_exp_share,
                "value": hh_exp_share,
            }

            cleaned_data.append(entry)
            del entry

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


def data_cleaning_ggdc(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from Groningen Growth and Development Center (ggdc) to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from GGDC

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
    print("this is the data_cleaning_ggdc")


# Define data restructure function
def data_restructure(
    cleaned_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
      '''
          Time,1950,1951,1952,...
          Share of VA Industry[Africa],x,x,x,...
          Share of VA Industry[AsiaPacific],x,x,x,...
          ......
          Share of VA Industry[WestEU],x,x,x,...
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
    logging.info("Group cleaned data by all dimensions")
    cleaned_data_groups = cleaned_data.groupby(["un_region", "year"])
    for region in regions:
        entry = {
            f"parameter": f"Share of Household Expenditure in GDP[{region}]",
        }
        entry_gdp = {
            f"parameter": f"GDP in 2015 $[{region}]",
        }
        entry_hh_exp = {
            f"parameter": f"Household Expenditure in 2015 $[{region}]",
        }
        for year in range(1900, 2101):
            if year in year_avail:
                try:
                    data_by_all = cleaned_data_groups.get_group(
                        (
                            region,
                            year,
                        )
                    )
                except KeyError:
                    entry[year] = ""
                    entry_gdp[year] = ""
                    entry_hh_exp[year] = ""
                    continue

                entry[year] = data_by_all["value"].values[0]
                entry_gdp[year] = data_by_all["gdp"].values[0]
                entry_hh_exp[year] = data_by_all["hh_exp"].values[0]
            else:
                entry[year] = ""
                entry_gdp[year] = ""
                entry_hh_exp[year] = ""

        restructured_data.extend([entry, entry_gdp, entry_hh_exp])
        del entry, entry_gdp, entry_hh_exp

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_hh_exp_share = data_cleaning(
    raw_data=raw_hh_exp_share,
    raw_data_source=data_source,
    concordance=concordance_table,
    raw_data_dep=raw_hh_exp_share_dep,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_gdp = data_restructure(
    cleaned_data=cleaned_hh_exp_share,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_gdp.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_by_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
