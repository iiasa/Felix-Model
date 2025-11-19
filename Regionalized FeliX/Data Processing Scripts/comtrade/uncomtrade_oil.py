# -*- coding: utf-8 -*-
"""
Created: Mon 9 Septembert 2024
Description: Scripts to process comtrade data among 5 FeliX regions
Scope: FeliX model regionalization, module comtrade 
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

# predefine the data source of comtrade
# data sources are wittgensteinwittgenstein, world_bank, unpd, ggdc (Groningen Growth and Development Center)
data_source = "un_comtrade"
variable = "oil_comtrade"
download_method = "manually"

# read config.yaml file
yaml_dir = Path("scripts/comtrade/config.yaml")
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

# Read raw data
logging.info(f"Read raw data")
raw_comtrade = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "ggdc":
    logging.info("4 data source is ggcd")
elif raw_data_info["datasource"] == "un_comtrade":
    for raw_data_file in raw_data_files:
        if download_method == "manually":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            raw_comtrade_ = pd.read_excel(
                path_raw_data_folder / raw_data_file,
                sheet_name="Sheet1",
            )
            raw_comtrade = pd.concat(
                [
                    raw_comtrade,
                    raw_comtrade_[
                        [
                            "period",
                            "reporterISO",
                            "reporterDesc",
                            "partnerISO",
                            "partnerDesc",
                            "partner2ISO",
                            "partner2Desc",
                            "qtyUnitAbbr",
                            "qty",
                        ]
                    ],
                ],
                ignore_index=True,
            )
            del raw_comtrade_
        else:
            logging.warning("The download method is not API")
            raise KeyError

logging.info("convert alternative units into kg")
units = np.unique(raw_comtrade["qtyUnitAbbr"].dropna())
raw_comtrade_groups = raw_comtrade.groupby("qtyUnitAbbr")
raw_comtrade = pd.DataFrame()
for unit in units:
    if unit in ["kg", "l"]:
        raw_comtrade_unit = raw_comtrade_groups.get_group(unit).astype({"qty": "float"})
        if unit == "l":
            raw_comtrade_unit["qty"] = (
                raw_comtrade_unit["qty"] * 0.85984522
            )  # convert liter to kg
            raw_comtrade_unit["qtyUnitAbbr"] = "kg"
        raw_comtrade = pd.concat(
            [raw_comtrade, raw_comtrade_unit],
            ignore_index=True,
        )
        del raw_comtrade_unit
unit = "kg"

logging.info("All alternative units have been converted into kg")

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
    elif raw_data_source == "un_comtrade":
        cleaned_data = data_cleaning_comtrade(
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


def data_cleaning_comtrade(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from UN COMTRADE to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from UN COMTRADE

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
        raw_data_merge_rep = pd.merge(
            raw_data,
            concordance[["region", "un_region"]].rename(
                columns={
                    "un_region": "un_region_reporter",
                },
            ),
            left_on="reporterDesc",
            right_on="region",
        ).rename(
            columns={
                "period": "year",
            }
        )
        raw_data_merge_part = pd.merge(
            raw_data_merge_rep,
            concordance[["region", "un_region"]].rename(
                columns={
                    "un_region": "un_region_partner",
                },
            ),
            left_on="partnerDesc",
            right_on="region",
        )
        raw_data_merge_part2 = pd.merge(
            raw_data_merge_part,
            concordance[["region", "un_region"]].rename(
                columns={
                    "un_region": "un_region_partner2",
                },
            ),
            left_on="partner2Desc",
            right_on="region",
        )
        raw_data_merge = raw_data_merge_part2
    else:
        raise KeyError("No data downloaded via API from UN Comtrade")

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(
        ["un_region_reporter", "un_region_partner", "un_region_partner2", "year"]
    )
    cleaned_data = []
    for year in years:
        for reporter in regions:
            for partner in regions:
                for partner2 in regions:
                    try:
                        raw_data_year = raw_data_groups.get_group(
                            (
                                reporter,
                                partner,
                                partner2,
                                year,
                            )
                        )
                    except KeyError:
                        continue
                    entry = {
                        "year": year,
                        "reporter": reporter,
                        "partner": partner,
                        "partner2": partner2,
                        "unit": unit,
                        "value": raw_data_year["qty"].sum(),
                    }
                    cleaned_data.append(entry)
                    del entry

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
          Oil Comtrade[Africa, Africa, Africa],x,x,x,...
          Oil Comtrade[Africa, Africa,AsiaPacific],x,x,x,...
          ......
          Oil Comtrade[WestEu, WestEU,WestEu],x,x,x,...
          Oil Comtrade[WestEu, WestEu,World],x,x,x,...
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
    logging.info("Group cleaned data by regions")

    restructured_data = []
    logging.info("Group cleaned data by all dimensions")
    cleaned_data_groups = cleaned_data.groupby(
        ["year", "reporter", "partner", "partner2"]
    )
    for reporter in regions:
        for partner in regions:
            # for partner2 in regions:
            entry = {
                f"parameter": f"Oil Comtrade {unit}[{reporter},{partner},World]",
            }

            for year in range(1900, 2101):
                try:
                    data_by_all = cleaned_data_groups.get_group(
                        (
                            year,
                            reporter,
                            partner,
                            "World",
                        )
                    )
                    entry[year] = data_by_all["value"].values[0]
                except KeyError:
                    entry[year] = ""

            restructured_data.append(entry)
            del entry

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_trade = data_cleaning(
    raw_data=raw_comtrade,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

logging.info("Start restructing the cleaned data based on the specified concordance")
restructured_trade = data_restructure(
    cleaned_data=cleaned_trade,
)
logging.info(f"Finish restructuring the cleaned data")

logging.info(f"Start writing the restructured data")
restructured_trade.to_csv(
    path_clean_data_folder.joinpath(f"{variable}_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
