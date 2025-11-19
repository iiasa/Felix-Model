# -*- coding: utf-8 -*-
"""
Created: Wed 06 December 2023
Description: Scripts to aggregate population data by region from Wittgenstein database to FeliX regions
Scope: FeliX model regionalization, module population 
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
raw_data_file = "wcde_data_1950_2020.csv"

# set paths of concordance table
felix_concordance = "concordance"
path_concordance_folder = root_path_raw.joinpath(
    f"version_{version}/{felix_concordance}"
)
concordance_file = "wittgenstein_regions_to_5_un_regions.csv"


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
    skiprows=8,
    encoding="utf-8",
)
logging.info(f"Finish reading raw data")


logging.info("Start reading condordance table")
concordance_table = pd.read_csv(
    path_concordance_folder / concordance_file,
    encoding="utf-8",
)
logging.info(f"Finish reading concordance table")

age_cohorts = [
    "0--4",
    "5--9",
    "10--14",
    "15--19",
    "20--24",
    "25--29",
    "30--34",
    "35--39",
    "40--44",
    "45--49",
    "50--54",
    "55--59",
    "60--64",
    "65--69",
    "70--74",
    "75--79",
    "80--84",
    "85--89",
    "90--94",
    "95--99",
    "100+",
]


# Define cleaning function
def data_aggregation(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
):
    """
    To transfer raw data from Wittgenstein database into FeliX region classification

    Parameter
    ---------
    raw_data: pd.DataFrame
        Data downloaded directly from Wittgenstein database.
        The resolutions of data in raw data should be consistent with the concordance table, vice versa

    concordance: pd.DataFrame
        The concordance table that links Wittgenstein regions and FeliX regions
        The FeliX regions are Africa, AsiaPacific, EastEu, LAC (latin american and the caribbean),
        WestEu, which follow UN classifications

    Returns
    -------
    cleaned data in pd.Dataframe

    """
    logging.info("Merge raw data with the concordance based on the regional match")
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["region", "un_region"]],
        left_on="Area",
        right_on="region",
    )

    logging.info("Check whethere there is any region missing after merging")
    missing_regions = [
        region
        for region in np.unique(raw_data["Area"])
        if region not in np.unique(raw_data_merge["region"])
    ]
    if missing_regions:
        logging.warning(
            f"{len(missing_regions)} regions are missing after merging. They are {missing_regions}"
        )
        raise ValueError
    else:
        logging.info("No region is missing after merging")

    logging.info("Sum up values into FeliX regional classifications")
    aggregated_data = (
        raw_data_merge.groupby(
            [
                "Year",
                "Age",
                "Sex",
                "un_region",
            ],
        )["Population"]
        .sum()
        .reset_index()
    )
    aggregated_data["Population"] *= 1000
    aggregated_data = aggregated_data.astype({"Population": int})
    aggregated_data["unit"] = "person"
    aggregated_data = aggregated_data.rename(
        str.lower,
        axis="columns",
    )

    logging.info("Correct the sort of age cohorts")
    aggregated_data_age = pd.DataFrame()
    for age_cohort in age_cohorts:
        aggregated_data_age = pd.concat(
            [
                aggregated_data_age,
                aggregated_data.loc[aggregated_data["age"] == age_cohort,],
            ],
            ignore_index=True,
        )
    del aggregated_data

    return aggregated_data_age


# Define data restructuring function
def data_restructure(
    clean_data: pd.DataFrame,
    **kwargs,
):
    """
    To restructure data cleaned via the data_aggregation function into the format:
    '''
        Time,1950,1951,1952,...
        Population,x,x,x,...
        Population Cohorts[Africa,female,"0-4"],x,x,x,...
        Population Cohorts[Africa,male,"0-4"],x,x,x,...
        ...
        Population Corhorts[WestEU,female,"100+"],x,x,x,...
        Population Corhorts[WestEU,male,"100+"],x,x,x,...
    '''
    The restructured data will be used as historic data for data calibration


    Parameter
    ---------
    clean_data: pd.DataFrame
        Data cleaned via the data_aggregation function.

    **kwargs
        Other arguments that may be used to restructure the clean data

    Returns
    -------
    restructured data in pd.Dataframe

    """
    columns_time = [year for year in range(1900, 2021)]
    regions = np.unique(clean_data["un_region"])
    genders = np.unique(clean_data["sex"])

    structured_data = []
    for region in regions:
        for gender in genders:
            for age_cohort in age_cohorts:
                facts = clean_data.loc[
                    (clean_data["un_region"] == region)
                    & (clean_data["sex"] == gender)
                    & (clean_data["age"] == age_cohort)
                ]
                fact_years = np.array(facts["year"])

                facts_structured = {}
                for column_year in columns_time:
                    if column_year in fact_years:
                        facts_structured[column_year] = facts.loc[
                            facts["year"] == column_year,
                            "population",
                        ].values[0]
                    else:
                        facts_structured[column_year] = None
                facts_structured[
                    "parameter"
                ] = f"Population Cohorts[{region},{gender.lower()},'{age_cohort.replace('--','-')}']"

                structured_data.append(
                    facts_structured,
                )
                del facts_structured, fact_years, facts

    structured_data = pd.DataFrame(structured_data)
    structured_data = structured_data.set_index("parameter")
    return structured_data


# Start cleaning the raw data
logging.info("Start cleaning the raw data based on the specified concordance")
clean_data = data_aggregation(
    raw_population,
    concordance_table,
)
logging.info("Finish data cleaning")

logging.info("Write clean data into a .csv file")
clean_data.to_csv(
    path_clean_data_folder.joinpath("population_by_felix_region.csv"),
    encoding="utf-8",
    index=False,
)
logging.info("Finish writing clean data")
logging.info("Clean procedure is done!")

# Start structuring the clean data, which will be used as historic data
logging.info("Start cleaning the raw data based on the specified concordance")
restructured_data = data_restructure(
    clean_data,
)
restructured_data.to_csv(
    path_clean_data_folder.joinpath("population_by_time_series.csv"),
    encoding="utf-8",
)
