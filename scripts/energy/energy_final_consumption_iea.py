# -*- coding: utf-8 -*-
"""
Created: Mon 13 May 2024
Description: Scripts to aggregate final energy consumption data by sector and region from IEA to FeliX regions
Scope: FeliX model regionalization, module energy 
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import logging
from pathlib import Path

import numpy as np
import pandas as pd

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# set paths of data
root_path_raw = Path("scripts")
root_path_clean = Path("scripts")
felix_module = "energy"

# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = root_path_raw.joinpath(
    f"{felix_module}/Total final consumption data at country level"
)
path_clean_data_folder = root_path_clean.joinpath(felix_module)

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


# Define 5 regions
regions = [
    "Africa",
    "AsiaPacific",
    "EastEu",
    "LAC",
    "WestEu",
]


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
    logging.info("Specify sectors")
    sectors = []
    for column_name in raw_data.columns:
        if column_name not in ["year", "units", "region", "location"]:
            sectors.append(column_name)
    years = np.unique(raw_data["year"])[:-1]  # omit 2022

    raw_data_groups = raw_data.groupby(["region", "year"])
    cleaned_fact = []
    for region in regions:
        for year in years:

            raw_data_region = raw_data_groups.get_group((region, year))
            for sector in sectors:
                entry = {
                    "region": region,
                    "year": year,
                    "sector": sector,
                    "unit": "TJ",
                    "value": raw_data_region[sector].sum(),
                }

                cleaned_fact.append(entry)
                del entry, sector
            del raw_data_region

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
        Final Energy Consumption[Africa,sector 1],x,x,x,...
        Final Energy Consumption[Africa,sector 2],x,x,x,...
        ...
        Final Energy Consumption[WestEu,sector n-1],x,x,x,...
        Final Energy Consumption[WestEu,sector n],x,x,x,...
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
    sectors = np.unique(clean_data["sector"])
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "sector", "year"])
    structured_data = []
    for region in regions:
        for sector in sectors:
            entry = {
                "parameter (unit: TJ)": f"Final Energy Consumption[{region},{sector.capitalize()}]",
            }
            for year in range(1900, 2101):
                if year in years:
                    cleaned_fact = clean_data_groups.get_group((region, sector, year))
                    entry[year] = cleaned_fact["value"].values[0]
                else:
                    entry[year] = np.nan

                del year

            structured_data.append(entry)
            del entry, sector

    structured_data = pd.DataFrame(structured_data)

    return structured_data


# Start cleaning the raw data
logging.info("Start script to aggregate final energy consumption data from IEA")
logging.info("Read raw final energy consumption data")
raw_fact = pd.DataFrame()
for path_region in path_raw_data_folder.glob(f"*States"):
    if "Africa" in str(path_region):
        region = "Africa"
    elif "Pacific" in str(path_region):
        region = "AsiaPacific"
    elif "East" in str(path_region):
        region = "EastEu"
    elif "Latin" in str(path_region):
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

        raw_fact_location["region"] = region
        raw_fact_location["location"] = location

        raw_fact = pd.concat(
            [raw_fact, raw_fact_location],
            ignore_index=True,
        )
logging.info(f"Finish reading raw data")

logging.info(f"Start cleaning the raw data")
cleaned_fact = data_cleaning(raw_fact)
logging.info("Finish data cleaning")

logging.info(f"Start restructuring the cleaned data")
restructured_fact = data_restructure(cleaned_fact)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
restructured_fact.to_csv(
    path_clean_data_folder.joinpath("final_energy_consumption_by_region.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
