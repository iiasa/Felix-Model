# -*- coding: utf-8 -*-
"""
Created: Mon 28 October 2024
Description: Scripts to aggregate energy data by source and region from IRENA to FeliX regions
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
data_source = "irena"
variable = "irena_data"
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
raw_energy = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("3 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    logging.info("3 data source is unpd")
elif raw_data_info["datasource"] == "irena":
    for raw_data_file in raw_data_files:
        if download_method == "manually":
            logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
            raw_energy_ = pd.read_excel(
                path_raw_data_folder / raw_data_file, sheet_name="Country"
            )

            raw_energy = pd.concat(
                [raw_energy, raw_energy_],
                ignore_index=True,
            )
            del raw_energy_
        else:
            logging.warning("The download method is not API")
            raise KeyError

# Read raw data
logging.info(f"Read dependent raw data")
if raw_data_dependency:
    raw_energy_dep = pd.DataFrame()
    if raw_data_info["datasource"] == "wittgenstein":
        logging.info("1 data source is wittgenstein")
    elif raw_data_info["datasource"] == "world_bank":
        logging.info("1 data source is world_bank")
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

    concordance: pd.DataFrame
        Concordance table between countries and 5 UN regions

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    cleaned data in pd.Dataframe

    """
    raw_data.columns = raw_data.columns.str.lower()
    logging.info("Specify energy sources")
    energy_sources = np.unique(raw_data["group technology"])

    logging.info("Specify available years")
    years = np.unique(raw_data["year"])

    logging.info("Merge cleaned data with regional concordance")
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["country", "un_region"]],
        on="country",
    )
    del raw_data

    raw_data_groups = raw_data_merge.groupby(["un_region", "group technology", "year"])
    cleaned_fact = []
    for region in regions:
        for energy_source in energy_sources:
            for year in years:
                try:
                    raw_data_region = raw_data_groups.get_group(
                        (region, energy_source, year)
                    )
                except KeyError:
                    continue

                entry = {
                    "region": region,
                    "energy_source": energy_source,
                    "year": year,
                    "electricity generation (GWh)": raw_data_region[
                        "electricity generation (gwh)"
                    ].sum(),
                    "electricity installed capacity (MW)": raw_data_region[
                        "electricity installed capacity (mw)"
                    ].sum(),
                    "heat generation (TJ)": raw_data_region[
                        "heat generation (tj)"
                    ].sum(),
                }

                cleaned_fact.append(entry)
                del entry, raw_data_region

    cleaned_fact = pd.DataFrame(cleaned_fact)
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
        Electricity generation (GWh)[Africa,energy source 1],x,x,x,...
        Electricity generation (GWh)[Africa,energy source 2],x,x,x,...
        ...
        Electricity generation (GWh)[WestEu,energy source n-1],x,x,x,...
        Electricity generation (GWh)[WestEu,energy source n],x,x,x,...
        Electricity installed capacity (MW)[Africa,energy source 1],x,x,x,...
        Electricity installed capacity (MW)[Africa,energy source 2],x,x,x,...
        ...
        Electricity installed capacity (MW)[WestEu,energy source n-1],x,x,x,...
        Electricity installed capacity (MW)[WestEu,energy source n],x,x,x,...
        Heat generation (TJ)[Africa,energy source 1],x,x,x,...
        Heat generation (TJ)[Africa,energy source 2],x,x,x,...
        ...
        Heat generation (TJ)[WestEu,energy source n-1],x,x,x,...
        Heat generation (TJ)[WestEu,energy source n],x,x,x,...
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
            entry_ele_gen = {
                "parameter": f"Electricity generation (GWh)",
                "region": region,
                "energy_source": energy_source,
            }
            entry_ele_cap = {
                "parameter": f"Electricity installed capacity (MW)",
                "region": region,
                "energy_source": energy_source,
            }
            entry_heat_gen = {
                "parameter": f"Heat generation (TJ)",
                "region": region,
                "energy_source": energy_source,
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_fact = clean_data_groups.get_group(
                            (region, energy_source, year)
                        )
                        entry_ele_gen[year] = cleaned_fact[
                            "electricity generation (GWh)"
                        ].values[0]
                        entry_ele_cap[year] = cleaned_fact[
                            "electricity installed capacity (MW)"
                        ].values[0]
                        entry_heat_gen[year] = cleaned_fact[
                            "heat generation (TJ)"
                        ].values[0]
                    except KeyError:
                        entry_ele_gen[year] = np.nan
                        entry_ele_cap[year] = np.nan
                        entry_heat_gen[year] = np.nan
                else:
                    entry_ele_gen[year] = np.nan
                    entry_ele_cap[year] = np.nan
                    entry_heat_gen[year] = np.nan

                del year

            structured_data.append(entry_ele_gen)
            structured_data.append(entry_ele_cap)
            structured_data.append(entry_heat_gen)
            del entry_ele_gen, entry_ele_cap, entry_heat_gen, energy_source

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info(f"Start cleaning the raw data")
cleaned_fact = data_cleaning(raw_data=raw_energy, concordance=concordance_table)
logging.info("Finish data cleaning")

logging.info(f"Start restructuring the cleaned data")
restructured_fact = data_restructure(cleaned_fact)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_fact.to_csv(
    path_clean_data_folder.joinpath(f"energy_data_time_series_{data_source}.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
