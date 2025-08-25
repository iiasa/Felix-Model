# -*- coding: utf-8 -*-
"""
Created: Wed 21 Feburary 2024
Description: Scripts to aggregate gdp data by region from UNPD to 5 FeliX regions
Scope: FeliX model regionalization, module gdp
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

# import pyam
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

project_name = "felix_regionalization"

# predefine the data source of gdp
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "world_bank"
variable = "gdp_2015usd"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/gdp/config.yaml")
with open(yaml_dir, "r") as dimension_file:
    data_info = yaml.safe_load(dimension_file)

version = data_info["version"]
felix_module = data_info["module"]
# Any path consists of at least a root path, a version path, a module path
path_clean_data_folder = Path(data_info["data_root_path"]).joinpath(
    f"clean_data/{project_name}/{version}/{felix_module}"
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
path_raw_data_folder = Path(data_info["data_root_path"]).joinpath(
    f"raw_data/{project_name}/{version}/{felix_module}"
)
raw_data_files = raw_data_info["data_file"]
logging.info("Path to raw data set")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path(data_info["data_root_path"]).joinpath(
    f"raw_data/{project_name}/{version}/concordance"
)
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
if "ipcc_r6" in concordance_file:
    regions = data_info["dimension"]["ipcc_r6"]
    final_region_name = "ipcc_r6"
else:
    regions = data_info["dimension"]["region"]
    final_region_name = "un_regions"
logging.info("Extracted dimensions of regions")

# Read raw data
logging.info(f"Read raw data")
raw_gdp = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "api":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            with open(path_raw_data_folder / raw_data_file) as fact_file:
                raw_gdp_ = json.load(fact_file)
            for data_point in raw_gdp_:
                if data_point["value"] == "":
                    data_point["value"] = np.nan
                    del data_point

            raw_gdp_ = pd.DataFrame(raw_gdp_)
            raw_gdp = pd.concat(
                [raw_gdp, raw_gdp_],
                ignore_index=True,
            )
            del raw_gdp_
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
# concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
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
    if "ipcc_r6" in concordance.columns:
        final_region_name = "ipcc_r6"
    else:
        final_region_name = "un_region"

    if download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["location", final_region_name]],
            left_on="country",
            right_on="location",
        ).rename(
            columns={
                "time": "year",
            }
        )
    else:
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["country", final_region_name]],
            on="country",
        ).rename(
            columns={
                "time": "year",
            }
        )

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby([final_region_name, "year"])
    cleaned_data = pd.DataFrame()
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
                if (
                    raw_data_region_year["value"].isna().sum()
                    < len(raw_data_region_year) * 0.2
                ):  # make sure at least 80% countries in a region have available GDP values

                    entry = {
                        final_region_name: region,
                        "year": year,
                        "value": raw_data_region_year["value"].sum(),
                        "unit": unit,
                    }

                    cleaned_data_.append(entry)
                    del entry
                else:
                    cleaned_data_ = []
                    break
            else:
                entry_glb = [
                    {
                        final_region_name: region,
                        "year": year,
                        "value": raw_data_region_year["value"].sum(),
                        "unit": unit,
                    }
                ]  # insert Global GDP
                cleaned_data = pd.concat(
                    [cleaned_data, pd.DataFrame(entry_glb)],
                    ignore_index=True,
                )
                del entry_glb

        # balance the global GDP
        if cleaned_data_:
            raw_global = raw_data_groups.get_group(
                (
                    "World",
                    year,
                )
            )
            index_value = [
                pos
                for pos, column_name in enumerate(raw_global.columns)
                if column_name == "value"
            ][0]
            raw_global_data = raw_global.iloc[0, index_value]
            cleaned_data_ = pd.DataFrame(cleaned_data_)
            # calculate the scaling factor to balance the global GDP
            sf = raw_global_data / cleaned_data_["value"].sum()
            cleaned_data_["value"] = cleaned_data_["value"] * sf

            cleaned_data = pd.concat(
                [cleaned_data, cleaned_data_],
                ignore_index=True,
            )
            del cleaned_data_, raw_global, raw_global_data, index_value, sf

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


def data_cleaning_iiasa(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from IIASA projection (SSP2) to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from IIASA projection (SSP2)

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
        concordance[["country", "un_region"]],
        left_on="region",
        right_on="country",
    )
    raw_data_groups = raw_data_merge.groupby(["model", "un_region"])
    cleaned_data = []
    for region in regions:
        raw_data_region_var = raw_data_groups.get_group(
            (
                "IIASA GDP 2023",
                region,
            )
        )

        for column in raw_data_region_var.columns:
            if (type(column) is int) and (column > 2020):
                entry = {
                    "un_region": region,
                }

                raw_data_region_var_year = (
                    raw_data_region_var[column].sum() * 1000000000
                )  # convert unit to person

                entry["year"] = column
                entry["value"] = raw_data_region_var_year

                entry["unit"] = unit

                cleaned_data.append(entry)
                del entry

    cleaned_data = pd.DataFrame(cleaned_data)

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
          Gross World Product in <unit>[Region 1],x,x,x,...
          Gross World Product in <unit>[Region 2],x,x,x,...
          ......
          Gross World Product in <unit>[Region N],x,x,x,...
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
    if "ipcc_r6" in cleaned_data.columns:
        final_region_name = "ipcc_r6"
    else:
        final_region_name = "un_region"

    cleaned_data_groups = cleaned_data.groupby([final_region_name, "year"])
    for region in regions:
        if variable == "gdp_2015usd":
            entry = {
                "parameter": f"Gross World Product in 2015 USD[{region}]",
            }
        elif variable == "gdp_ppp":
            entry = {
                "parameter": f"Gross World Product in 2017 international $[{region}]",
            }
        for year in range(1900, 2101):
            if year in year_avail:
                data_by_all = cleaned_data_groups.get_group(
                    (
                        region,
                        year,
                    )
                )
                index_value = [
                    pos
                    for pos, column_name in enumerate(data_by_all.columns)
                    if column_name == "value"
                ][0]
                entry[year] = data_by_all.iloc[0, index_value]
                del index_value
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
cleaned_gdp = data_cleaning(
    raw_data=raw_gdp,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

##################################################################################################
including_iiasa = False
# if including_iiasa:
#     logging.info(f"Add SSP2 gdp data from IIASA based on the specified concordance")
#     logging.info(f"Load IIASA data")
#     iiasa_data_file = "1706548837040-ssp_basic_drivers_release_3.0_full.csv"
#     iiasa_data = pyam.IamDataFrame(
#         data=Path("data_regionalization_raw")
#         / f"version_{version}"
#         / "iiasa"
#         / iiasa_data_file
#     )
#     logging.info(f"Select gdp-related data from IIASA dataset")
#     iiasa_raw_data = (
#         iiasa_data.filter(scenario="SSP2", variable="GDP|PPP")
#         .timeseries()
#         .reset_index()
#     )

#     logging.info(f"Load concordance between IIASA regions and FeliX regions")
#     concordance_file_iiasa = "iiasa_countries_to_5_un_regions.csv"
#     concordance_table_iiasa = pd.read_csv(
#         path_concordance_folder / concordance_file_iiasa,
#         encoding="utf-8",
#     )
#     concordance_table_iiasa = concordance_table_iiasa.dropna().reset_index(drop=True)
#     concordance_table_iiasa["un_region_code"] = concordance_table_iiasa[
#         "un_region_code"
#     ].astype("int")
#     logging.info(f"Finish reading concordance table for IIASA")

#     cleaned_data_iiasa = data_cleaning_iiasa(
#         raw_data=iiasa_raw_data,
#         concordance=concordance_table_iiasa,
#     )
##################################################################################################

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
if including_iiasa:
    restructured_gdp = data_restructure(
        cleaned_data=pd.concat([cleaned_gdp, cleaned_data_iiasa])
    )
    logging.info(f"Finish restructuring the cleaned data")

    logging.info(f"Start writing the restructured data")
    restructured_gdp.to_csv(
        path_clean_data_folder.joinpath(
            f"{variable}_by_time_series_{data_source}_iiasa.csv"
        ),
        encoding="utf-8",
        index=False,
    )
    logging.info(f"Finish writing the restructured data")
    logging.info(f"The whole procedures of cleaning and restructuring data are done!")
else:
    restructured_gdp = data_restructure(
        cleaned_data=cleaned_gdp,
    )
    logging.info(f"Finish restructuring the cleaned data")

    logging.info(f"Start writing the restructured data")
    restructured_gdp.to_csv(
        path_clean_data_folder.joinpath(
            f"{variable}_by_time_series_{data_source}_{final_region_name}.csv"
        ),
        encoding="utf-8",
        index=False,
    )
    logging.info(f"Finish writing the restructured data")
    logging.info(f"The whole procedures of cleaning and restructuring data are done!")
