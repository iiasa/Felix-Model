# -*- coding: utf-8 -*-
"""
Created: Thur 11 January 2024
Description: Scripts to calculate GDP per capital by FeliX regions
Scope: FeliX model regionalization, module GDP
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

project_name = "felix_regionalization"

# set paths of data
root_path = Path("C:/Users/yequanliang/OneDrive - IIASA/data")
root_path_raw = root_path / "raw_data"
root_path_clean = root_path / "clean_data"
version = "v.11.2023"
felix_module = "gdp"

# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = root_path_raw.joinpath(
    f"{project_name}/{version}/{felix_module}"
)
path_clean_data_folder = root_path_clean.joinpath(
    f"{project_name}/{version}/{felix_module}"
)
raw_data_file = "gdp_2015_usd.csv"
raw_unit = "2015_USD"

# set paths of dependant data --- mostly is population by country
felix_module_dep = "population"

# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder_dep = root_path_raw.joinpath(
    f"{project_name}/{version}/{felix_module_dep}"
)
raw_data_file_dep = "population_world_bank.csv"

# set paths of concordance table
felix_concordance = "concordance"
path_concordance_folder = root_path_raw.joinpath(
    f"{project_name}/{version}/{felix_concordance}"
)
concordance_file = "world_bank_countries_to_ipcc_r6.csv"


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


# Read raw data and raw dependant data
logging.info("Start script to aggregate gdp data by region from World Bank")
logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
logging.info(f"Read raw data")
raw_gdp = pd.read_csv(
    path_raw_data_folder / raw_data_file,
    skiprows=4,
    encoding="utf-8",
)
logging.info(f"Finish reading raw data")

logging.info(f"Read raw dependant data")
raw_population = pd.read_csv(
    path_raw_data_folder_dep / raw_data_file_dep,
    skiprows=4,
    encoding="utf-8",
)
logging.info(f"Finish reading raw dependant data")

logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
# concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")

if "ipcc_r6" in concordance_table.columns:
    final_region_name = "ipcc_r6"
    regions = [
        "Africa",
        "Asia and Pacific",
        "Developed Countries",
        "Eastern Europe and West-Central Asia",
        "Latin America and Caribbean",
        "Middle East",
        "World",
    ]
else:
    final_region_name = "un_region"
    regions = [
        "Africa",
        "AsiaPacific",
        "EastEu",
        "LAC",
        "WestEu",
        "World",
    ]


# Define data restructuring function
def data_restructure(
    raw_data: pd.DataFrame,
    raw_data_dep: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
    '''
        Time,1950,1951,1952,...
        GWP per Capita[Africa],x,x,x,...
        GWP per Capita[AsiaPacific],x,x,x,...
        ......
        GWP per Capita[WestEU],x,x,x,...
    '''
    The restructured data will be used as historic data for data calibration


    Parameter
    ---------
    data: pd.DataFrame
        Raw data from the World Bank

    raw_data_dep: pd.DataFrame
        Raw dependant data from the World Bank

    Concordance: pd.DataFrame
        The concordance table that links WorldBank countries and FeliX regions
        The FeliX regions are Africa, AsiaPacific, EastEu, LAC (latin american and the caribbean),
        WestEu, which follow UN classifications

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    restructured data in pd.Dataframe

    """
    parameter = f"GWP per Capita in {raw_unit.replace('_',' ')}"
    structured_data = []
    for region in regions:
        structured_data.append({"parameter": f"{parameter}[{region}]"})

    for column in raw_data.columns:
        if column in [str(i) for i in range(1950, 2030)]:
            raw_data_year = pd.merge(
                raw_data[["Country Name", column]],
                raw_data_dep[["Country Name", column]],
                on="Country Name",
            )
            raw_data_year = raw_data_year.dropna()

            logging.info(
                f"Merge raw data with the concordance based on the regional match, for year {column}"
            )
            raw_data_merge = pd.merge(
                raw_data_year,
                concordance[["location", final_region_name]],
                left_on="Country Name",
                right_on="location",
            )

            logging.info(
                f"Calculate average life expectancy at birth by Felix region, for year {column}"
            )
            raw_data_group = raw_data_merge.groupby([final_region_name])

            for pos, region in enumerate(
                np.unique(
                    raw_data_merge[final_region_name],
                ),
            ):
                raw_data_region = raw_data_group.get_group(region)
                structured_data[pos][column] = (
                    raw_data_region[f"{column}_x"].sum()
                    / raw_data_region[f"{column}_y"].sum()
                )
                del pos, region, raw_data_region

    structured_data = pd.DataFrame(structured_data)
    structured_data["unit"] = raw_unit
    structured_data = structured_data.set_index("parameter")

    return structured_data


# Start structuring raw data, which will be used as historic data
logging.info("Start restructing the raw data based on the specified concordance")
restructured_data = data_restructure(
    raw_data=raw_gdp,
    raw_data_dep=raw_population,
    concordance=concordance_table,
)
logging.info("Finish restructing the raw data based on the specified concordance")

logging.info("Write structured data into a .csv file")
restructured_data.to_csv(
    path_clean_data_folder.joinpath(
        f"gdp_per_capita_{raw_unit}_by_time_series_{final_region_name}.csv"
    ),
    encoding="utf-8",
)
logging.info("Finish writing structured data.")


# # convert gdp to 2005 USD
# convert_rate = 0.835833
# currency_converted_data = restructured_data
# if raw_unit == "2015_USD":
#     for column in currency_converted_data.columns:
#         if column in [str(i) for i in range(1960, 2030)]:
#             currency_converted_data[column] = (
#                 currency_converted_data[column] * convert_rate
#             )
# currency_converted_data = currency_converted_data.reset_index()
# currency_converted_data["parameter"] = [
#     "GWP per Capita[Africa]",
#     "GWP per Capita[AsiaPacific]",
#     "GWP per Capita[EastEU]",
#     "GWP per Capita[LAC]",
#     "GWP per Capita[WestEU]",
# ]
# currency_converted_data["unit"] = ["2005 USD"] * 5
# currency_converted_data.to_csv(
#     path_clean_data_folder.joinpath(f"gdp_per_capital_in_2005_USD_by_time_series.csv"),
#     encoding="utf-8",
#     index=False,
# )
