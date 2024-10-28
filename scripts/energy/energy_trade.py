# -*- coding: utf-8 -*-
"""
Created: Thur 5 September 2024
Description: Scripts to aggregate energy trade data by source and region from IEA to FeliX regions
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
path_raw_data_folder = root_path_raw.joinpath(f"{felix_module}/energy_import_export")
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
    logging.info("Specify available years")
    years = np.unique(raw_data["year"])

    raw_data_groups = raw_data.groupby(["region", "year"])
    cleaned_fact = []
    for region in regions:
        for year in years:
            try:
                raw_data_region = raw_data_groups.get_group((region, year))
            except KeyError:
                continue

            entry = {
                "region": region,
                "energy_source": energy_source,
                "year": year,
                "unit": "TJ",
                "value": -raw_data_region[
                    "exports"
                ].sum(),  # -raw_data_region['imports'].sum(),
                # export only
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
        Total energy supply (TES) by source[Africa,energy source 1],x,x,x,...
        Total energy supply (TES) by source[Africa,energy source 1],x,x,x,...
        ...
        Total energy supply (TES) by source[WestEu,energy source n-1],x,x,x,...
        Total energy supply (TES) by source[WestEu,energy source n],x,x,x,...
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
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "year"])
    structured_data = []
    for region in regions:
        entry = {
            "parameter (unit: TJ)": f"{energy_source.capitalize()} Net Export",
            "region": region,
            "energy_source": energy_source,
        }
        for year in range(1900, 2101):
            if year in years:
                try:
                    cleaned_fact = clean_data_groups.get_group((region, year))
                    entry[year] = cleaned_fact["value"].values[0]
                except KeyError:
                    entry[year] = np.nan
            else:
                entry[year] = np.nan
            del year

        structured_data.append(entry)
        del entry

    structured_data = pd.DataFrame(structured_data)
    return structured_data


# Start cleaning the raw data
logging.info("Start script to aggregate final energy consumption data from IEA")
raw_fact = pd.DataFrame()
for path_region in path_raw_data_folder.glob(f"*"):
    if "Africa" in str(path_region):
        region = "Africa"
    elif "Asia" in str(path_region):
        region = "AsiaPacific"
    elif "easteu" in str(path_region):
        region = "EastEu"
    elif "Latin" in str(path_region):
        region = "LAC"
    elif "Western" in str(path_region):
        region = "WestEu"

    for path_fact in path_region.glob("*"):
        if "oil" in str(path_fact).lower():
            energy_source = "oil"

            location = str(path_fact).split("- ")[-1].split(".")[0]
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
    path_clean_data_folder.joinpath(f"export_{energy_source}_iea.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
