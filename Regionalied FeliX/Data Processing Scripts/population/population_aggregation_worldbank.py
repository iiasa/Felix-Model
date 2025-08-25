# -*- coding: utf-8 -*-
"""
Created: Tue 09 January 2024
Description: Scripts to aggregate population data by region from The World Bank to FeliX regions
Scope: FeliX model regionalization, module Population 
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
root_path_raw = Path("data_regionalization_raw")
root_path_clean = Path("data_regionalization_clean")
version = "nov2023"
felix_module = "population"

# Any path consists of at least a root path, a version path, a module path
path_raw_data_folder = root_path_raw.joinpath(f"version_{version}/{felix_module}")
path_clean_data_folder = root_path_clean.joinpath(f"version_{version}/{felix_module}")
raw_data_file = "population_world_bank.csv"

# set paths of concordance table
felix_concordance = "concordance"
path_concordance_folder = root_path_raw.joinpath(
    f"version_{version}/{felix_concordance}"
)
concordance_file = "world_bank_countries_to_5_un_regions.csv"


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


# Read raw data
logging.info(
    "Start script to aggregate population data by region from Wittgenstein database"
)
logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
logging.info(f"Read raw data")
raw_population = pd.read_csv(
    path_raw_data_folder / raw_data_file,
    skiprows=4,
    encoding="utf-8",
)
logging.info(f"Finish reading raw data")


logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
concordance_table = concordance_table.dropna()
concordance_table["un_region_code"] = concordance_table["un_region_code"].astype("int")
logging.info(f"Finish reading concordance table")


# Define data restructuring function
def data_restructure(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data into the format:
    '''
        Time,1950,1951,1952,...
        Population[Africa],x,x,x,...
        Population[AsiaPacific],x,x,x,...
        ......
        Population[WestEU],x,x,x,...
    '''
    The restructured data will be used as historic data for data calibration


    Parameter
    ---------
    data: pd.DataFrame
        Raw data from the World Bank

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
    # Clean data into a simplier format
    logging.info("Merge raw data with the concordance based on the regional match")
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["country", "un_region"]],
        left_on="Country Name",
        right_on="country",
    )

    logging.info("Sum up values into FeliX regional classifications")
    aggregated_data_ = raw_data_merge.groupby(
        [
            "un_region",
        ],
    )
    aggregated_data = pd.DataFrame()
    for column in raw_data_merge.columns:
        if column in [str(i) for i in range(1960, 2030)]:
            aggregated_data = pd.concat(
                [
                    aggregated_data,
                    aggregated_data_[column].sum(),
                ],
                axis=1,
            )
            aggregated_data = aggregated_data.astype({column: float})
    aggregated_data = aggregated_data.reset_index()
    aggregated_data["unit"] = "person"
    aggregated_data = aggregated_data.rename(
        str.lower,
        axis="columns",
    )
    parameter = [
        f"Population[{region}]"
        for region in np.unique(
            aggregated_data["index"],
        )
    ]
    aggregated_data["parameter"] = parameter
    aggregated_data = aggregated_data.drop(["index"], axis=1)
    structured_data = aggregated_data.set_index("parameter")
    return structured_data


# Start structuring raw data, which will be used as historic data
logging.info("Start restructing the raw data based on the specified concordance")
restructured_data = data_restructure(
    raw_data=raw_population,
    concordance=concordance_table,
)

restructured_data.to_csv(
    path_clean_data_folder.joinpath(f"population_by_time_series_worldbank.csv"),
    encoding="utf-8",
)
