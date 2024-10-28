# -*- coding: utf-8 -*-
"""
Created: Thur 22 August 2024
Description: Scripts to aggregate energy data by countries/regions from BP to FeliX regions
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
path_raw_data_folder = root_path_raw.joinpath(f"{felix_module}")
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
        if column_name not in ["year", "units", "total", "region", "location"]:
            energy_sources.append(column_name)
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

            for energy_source in energy_sources:
                entry = {
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
    energy_sources = np.unique(clean_data["energy_source"])
    years = np.unique(clean_data["year"])

    logging.info("Restructure cleaned data")
    clean_data_groups = clean_data.groupby(["region", "energy_source", "year"])
    structured_data = []
    for region in regions:
        for energy_source in energy_sources:
            entry = {
                "parameter (unit: TJ)": f"Total energy supply (TES) by source",
                "region": region,
                "energy_source": energy_source,
            }
            for year in range(1900, 2101):
                if year in years:
                    try:
                        cleaned_fact = clean_data_groups.get_group(
                            (region, energy_source, year)
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
logging.info("Start script to aggregate energy data from BP")
logging.info("Read raw data file")
file_path = path_raw_data_folder.joinpath(
    "Statistical Review of World Energy Data.xlsx"
)
facts_data = pd.ExcelFile(file_path)
logging.info(f"Finish reading raw data")

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path("data_regionalization_raw/version_nov2023/concordance")
concordance_file = "bp_countries_to_5_un_regions.csv"

concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


logging.info("Pre-specify name of variable and data sources")
variable = "coal consumption"
data_source = "bp"

logging.info("Extract raw fact data")
# Iterate over all sheet names in the Excel file
found_data = False
for sheet_name in facts_data.sheet_names:
    if variable in sheet_name.lower():
        found_data = True
        raw_fact = pd.read_excel(facts_data, sheet_name=sheet_name)
        raw_fact.columns = raw_fact.iloc[1, :].to_list()
        unit_fact = raw_fact.columns[0]

        raw_data_merge = pd.merge(
            raw_fact,
            concordance_table[["location", "un_region"]],
            left_on=unit_fact,
            right_on="location",
        )

        raw_data_merge = raw_data_merge.groupby("un_region").sum([raw_fact.columns[1]])
        raw_data_merge[unit_fact.lower().replace(" ", "_")] = variable
        raw_data_merge = raw_data_merge.reset_index()
        raw_data_merge = raw_data_merge[
            [raw_data_merge.columns[-1]] + raw_data_merge.columns.to_list()[:-1]
        ]

        logging.info("Write clean data into a .csv file")
        raw_data_merge.to_csv(
            path_clean_data_folder.joinpath(
                f"{variable.replace(' ','_')}_{unit_fact.lower().replace(' ','_')}_bp.csv"
            ),
            encoding="utf-8",
            index=False,
        )

        del raw_fact, unit_fact, raw_data_merge
        logging.info("Finish writing clean data")
        logging.info("Clean procedure is done!")
    else:
        continue

if found_data is False:
    raise KeyError(f"No sheet was found for {variable}")
