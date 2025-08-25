# -*- coding: utf-8 -*-
"""
Created: Fri 31 May 2024
Description: Scripts to aggregate final energy consumption data by source and region from IEA to FeliX regions
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
    f"{felix_module}/TFC by source at country level"
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
    logging.info("Specify energy sources")
    energy_sources = []
    for column_name in raw_data.columns:
        if column_name not in ["year", "units", "sector", "region", "location"]:
            energy_sources.append(column_name)
    logging.info("Specify available years")
    years = np.unique(raw_data["year"])

    raw_data_groups = raw_data.groupby(["sector", "region", "year"])
    cleaned_fact = []
    for sector in sectors:
        for region in regions:
            for year in years:
                try:
                    raw_data_region = raw_data_groups.get_group((sector, region, year))
                except KeyError:
                    continue
                for energy_source in energy_sources:
                    entry = {
                        "sector": sector,
                        "region": region,
                        "energy_source": energy_source,
                        "year": year,
                        "unit": "TJ",
                        "value": raw_data_region[energy_source].sum(),
                    }

                    cleaned_fact.append(entry)
                    del entry, energy_source
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
        TFC by source[Africa,sector 1],x,x,x,...
        TFC by source[Africa,sector 2],x,x,x,...
        ...
        TFC by source[WestEu,sector n-1],x,x,x,...
        TFC by source[WestEu,sector n],x,x,x,...
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
    clean_data_groups = clean_data.groupby(
        ["region", "sector", "energy_source", "year"]
    )
    structured_data = []
    for region in regions:
        for sector in sectors:
            for energy_source in energy_sources:
                entry = {
                    "parameter (unit: TJ)": f"Final Energy Consumption",
                    "region": region,
                    "sector": sector,
                    "energy_source": energy_source,
                }
                for year in range(1900, 2101):
                    if year in years:
                        try:
                            cleaned_fact = clean_data_groups.get_group(
                                (region, sector, energy_source, year)
                            )
                            entry[year] = cleaned_fact["value"].values[0]
                        except KeyError:
                            entry[year] = np.nan
                    else:
                        entry[year] = np.nan

                    del year

                structured_data.append(entry)
                del entry, energy_source

    structured_data = pd.DataFrame(structured_data)

    return structured_data


# Start cleaning the raw data
logging.info("Start script to aggregate final energy consumption data from IEA")
sectors = []
raw_fact = pd.DataFrame()
for path_sector in path_raw_data_folder.glob(f"*Sector"):
    sector = str(path_sector).split("\\")[-1][:-7]
    sectors.append(sector)
    logging.info(f"Read raw final energy consumption data by {sector}")
    for path_region in path_sector.glob(f"{sector}*"):
        if "Africa" in str(path_region):
            region = "Africa"
        elif "Pacific" in str(path_region):
            region = "AsiaPacific"
        elif "East" in str(path_region):
            region = "EastEu"
        elif "LAC" in str(path_region):
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

            raw_fact_location["sector"] = sector
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
    path_clean_data_folder.joinpath(f"tfc_by_source_by_region.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")
