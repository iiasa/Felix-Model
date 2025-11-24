# -*- coding: utf-8 -*-
"""
Created: Mon 27 May 2024
Description: Scripts to capital stock time series for 5 FeliX regions
Scope: FeliX model regionalization, module capital 
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

# predefine the data source of capital
# data sources are wittgensteinwittgenstein, world_bank, unpd, ggdc (Groningen Growth and Development Center)
data_source = "world_bank"
variable = "capital_depr"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/capital/config.yaml")
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
unit = raw_data_info["unit"]

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

logging.info("Extracting dimension information for data cleaning and restructing")

regions = data_info["dimension"]["region"]
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_capital = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "api":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            with open(path_raw_data_folder / raw_data_file) as fact_file:
                raw_capital_ = json.load(fact_file)
            for data_point in raw_capital_:
                if data_point["value"] == "":
                    data_point["value"] = np.nan
                    del data_point

            raw_capital_ = pd.DataFrame(raw_capital_)
            raw_capital = pd.concat(
                [raw_capital, raw_capital_],
                ignore_index=True,
            )
            del raw_capital_
        else:
            logging.warning("The download method is not API")
            raise KeyError
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "ggdc":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "manually":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            raw_capital_ = pd.read_excel(
                path_raw_data_folder / raw_data_file,
                sheet_name="Data",
            )

            raw_capital = pd.concat(
                [
                    raw_capital,
                    raw_capital_[
                        [
                            "country",
                            "year",
                            "ck",  # Capital stock at current PPPs (in mil. 2005US$)
                            "cgdpo",  # Output-side real GDP at current PPPs (in mil. 2005US$)
                        ]
                    ],
                ],
                ignore_index=True,
            )
            del raw_capital_
        else:
            logging.warning("The download method is not API")
            raise KeyError

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

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(["un_region", "year"])
    cleaned_data = []
    for year in years:
        cleaned_data_ = []
        for region in regions:
            raw_data_region_year = raw_data_groups.get_group(
                (
                    region,
                    year,
                )
            )
            if region != "World":  # distinguish the region
                if region == "LAC":
                    threshlod_nan = 0.3  # to make sure the number of countries with no data less than 20% of all countries of that region
                elif region == "AsiaPacific":
                    threshlod_nan = 0.3
                else:
                    threshlod_nan = 0.2

                if (
                    raw_data_region_year["value"].isna().sum()
                    < len(raw_data_region_year) * threshlod_nan
                ):  # make sure at least 80% countries in a region have available values
                    entry = {
                        "un_region": region,
                        "year": year,
                        "unit": unit,
                        "value": raw_data_region_year["value"].sum(),
                    }

                    cleaned_data_.append(entry)
                    cleaned_data.append(entry)
                    del entry

        if len(cleaned_data_) == 5:
            entry_glb = {
                "un_region": regions[-1],
                "year": year,
                "unit": unit,
                "value": pd.DataFrame(cleaned_data_)["value"].sum(),
            }  # insert Global GDP
            cleaned_data.append(entry_glb)
            del entry_glb

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
    if download_method == "manually":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", "un_region"]],
            on="country",
        )
    else:
        raise KeyError("No capital-related data downloaded via API")

    raw_data_groups = raw_data_merge.groupby(["un_region"])
    cleaned_data = []
    for region in regions:
        if region != "World":  # distinguish the region
            raw_data_region = raw_data_groups.get_group(region).dropna()
            years = np.unique(raw_data_region["year"])
            countries = np.unique(raw_data_region["country"])

            raw_data_region_groups = raw_data_region.groupby("year")
            for year in years:
                raw_data_region_year = raw_data_region_groups.get_group(year)
                if len(raw_data_region_year) == len(countries):
                    entry = {
                        "un_region": region,
                        "year": year,
                        "unit": unit,
                        "capital_depr": raw_data_region_year["ck"].sum()
                        * 1000000,  # convert million usd to usd
                        "gdp_output": raw_data_region_year["cgdpo"].sum()
                        * 1000000,  # convert million usd to usd
                        "capital_int": raw_data_region_year["ck"].sum()
                        / raw_data_region_year["cgdpo"].sum(),
                    }

                    cleaned_data.append(entry)
                    del raw_data_region_year, entry
            del raw_data_region, years, countries, raw_data_region_groups

        else:
            years = np.unique(raw_data_merge["year"])
            countries = np.unique(raw_data_merge["country"])

            raw_data_merge_groups = raw_data_merge.groupby("year")
            for year in years:
                raw_data_year = raw_data_merge_groups.get_group(year)
                if len(raw_data_year) == len(countries):
                    entry = {
                        "un_region": region,
                        "year": year,
                        "unit": unit,
                        "value": raw_data_year["ck"].sum()
                        * 1000000,  # convert million usd to usd
                        "gdp_output": raw_data_year["cgdpo"].sum()
                        * 1000000,  # convert million usd to usd
                        "capital_int": raw_data_year["ck"].sum()
                        / raw_data_year["cgdpo"].sum(),
                    }

                    cleaned_data.append(entry)
                    del entry, raw_data_year
            del years, countries, raw_data_merge_groups

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
          Capital[Africa],x,x,x,...
          Capital[AsiaPacific],x,x,x,...
          ......
          Capital[WestEU],x,x,x,...
          Output-side GDP[Africa],x,x,x,...
          ......
          Output-side GDP[World],x,x,x,...
          Capital intensity[Africa],x,x,x,...
          ......
          Capital intensity[World],x,x,x,..
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
        if variable == "capital_stock":
            entry = {
                f"parameter": f"Capital in {unit}[{region}]",
            }
            entry_gdp_output = {
                f"parameter": f"Output-side GDP in {unit}[{region}]",
            }
            entry_cap_int = {
                f"parameter": f"Capital Intensity[{region}]",
            }
        elif variable == "capital_depr":
            entry = {
                f"parameter": f"Capital Depreciation {unit}[{region}]",
            }
        elif variable == "capital_form_fix":
            entry = {
                f"parameter": f"Gross Fixed Capital Formation {unit}[{region}]",
            }
        elif variable == "capital_form_gross":
            entry = {
                f"parameter": f"Gross Capital Formation {unit}[{region}]",
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
                    if variable == "capital_stock":
                        entry_gdp_output[year] = ""
                        entry_cap_int[year] = ""
                    continue

                entry[year] = data_by_all["value"].values[0]
                if variable == "capital_stock":
                    entry_gdp_output[year] = data_by_all["gdp_output"].values[0]
                    entry_cap_int[year] = data_by_all["capital_int"].values[0]
            else:
                entry[year] = ""
                if variable == "capital_stock":
                    entry_gdp_output[year] = ""
                    entry_cap_int[year] = ""

        restructured_data.append(entry)
        if variable == "capital_stock":
            restructured_data.append(entry_gdp_output)
            restructured_data.append(entry_cap_int)

        del entry

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_gdp = data_cleaning(
    raw_data=raw_capital,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")


logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_gdp = data_restructure(
    cleaned_data=cleaned_gdp,
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
