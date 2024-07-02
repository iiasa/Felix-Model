# -*- coding: utf-8 -*-
"""
Created: Thur 08 Feburary 2024
Description: Scripts to aggregate population data by region from UNPD to 5 FeliX regions
Scope: FeliX model regionalization, module Population 
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyam
import yaml

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of population
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "unpd"
download_method = "api"

# read config.yaml file
yaml_dir = Path("scripts/population/config.yaml")
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
    if dataset["datasource"] == data_source:
        if dataset["download_via"] == download_method:
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

logging.info("Set concordance tables of regional classifications")
# set paths of concordance table
path_concordance_folder = Path(raw_data_info["root_path"]).joinpath(
    f"version_{version}/concordance"
)
concordance_file = raw_data_info["concordance"]
logging.info("Concordance tables of regional classifications set")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
genders = data_info["dimension"]["gender"]
age_cohorts = data_info["dimension"]["age"]
logging.info("Extracted dimensions of regions, genders, and ages")

# Read raw data
logging.info(f"Read raw data")
raw_population = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    logging.info("1 data source is wittgenstein")
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
elif raw_data_info["datasource"] == "unpd":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "api":
            raw_population_ = pd.read_csv(
                path_raw_data_folder / raw_data_file,
            )
            raw_population = pd.concat(
                [raw_population, raw_population_],
                ignore_index=True,
            )
            del raw_population_
        else:
            raw_population_ = pd.read_excel(
                path_raw_data_folder / raw_data_file,
                sheet_name="Estimates",
                skiprows=16,
            )
            raw_population_["sex"] = re.findall(
                r"_([A-Z]+)\.xlsx$",
                raw_data_file,
            )[0].lower()
            logging.info(f"Finish reading raw data")
            raw_population = pd.concat(
                [raw_population, raw_population_],
                ignore_index=True,
            )
            del raw_population_

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
    if download_method == "api":
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["region_api", "un_region"]],
            left_on="Location",
            right_on="region_api",
        ).rename(
            columns={
                "TimeLabel": "Year",
                "Sex": "sex",
                "AgeLabel": "age",
            }
        )
        raw_data_merge["sex"] = [value.lower() for value in raw_data_merge["sex"]]
    else:
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["region", "un_region"]],
            left_on="Region, subregion, country or area *",
            right_on="region",
        )

    years = np.unique(raw_data_merge["Year"])
    raw_data_groups = raw_data_merge.groupby(["un_region", "sex", "Year"])
    cleaned_data = []
    for region in regions:
        for sex in genders:
            for year in years:
                raw_data_region_year = raw_data_groups.get_group(
                    (
                        region,
                        sex,
                        year,
                    )
                )

                if download_method == "api":
                    raw_data_region_year_ages = (
                        raw_data_region_year.groupby(["age"])["Value"]
                        .sum()
                        .reset_index()
                    )
                    for age in age_cohorts:
                        age = age.replace("--", "-")
                        entry = {
                            "un_region": region,
                            "sex": sex,
                            "age": age,
                            "year": year,
                            "value": raw_data_region_year_ages.loc[
                                raw_data_region_year_ages["age"] == age,
                                "Value",
                            ].values[0],
                            "unit": "person",
                        }
                        cleaned_data.append(entry)
                        del entry, age
                else:
                    entry = {
                        "un_region": region,
                        "sex": sex,
                        "age": age,
                        "year": year,
                        "value": raw_data_region_year[age].sum()
                        * 1000,  # convert to person
                        "unit": "person",
                    }

                    cleaned_data.append(entry)
                    del entry, age

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


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
        concordance[["location", "un_region"]],
        left_on="region",
        right_on="location",
    )
    raw_data_groups = raw_data_merge.groupby(["un_region", "variable"])
    cleaned_data = []
    for region in regions:
        for variable in np.unique(raw_data_merge["variable"]):
            raw_data_region_var = raw_data_groups.get_group(
                (
                    region,
                    variable,
                )
            )
            for column in raw_data_region_var.columns:
                if (type(column) is int) and (column > 2020):
                    entry = {
                        "un_region": region,
                        "sex": variable.split("|")[1].lower(),
                        "age": variable.split("|")[-1].split(" ")[-1],
                    }

                    raw_data_region_var_year = (
                        raw_data_region_var[column].sum() * 1000000
                    )  # convert unit to person

                    entry["year"] = column
                    entry["value"] = raw_data_region_var_year

                    entry["unit"] = "person"

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
          Population[Africa],x,x,x,...
          Population[AsiaPacific],x,x,x,...
          ......
          Population[WestEU],x,x,x,...
          Population[World],x,x,x,...
          Population by Gender[Africa,female],x,x,x,...
          Population by Gender[Africa,male],x,x,x,...
          ......
          Population by Gender[WestEU,female],x,x,x,...
          Population by Gender[WestEU,male],x,x,x,...
          Population by Gender[World,female],x,x,x,...
          Population by Gender[World,male],x,x,x,...
          Population Cohorts[Africa,female,"0-4"],x,x,x,...
          Population Cohorts[Africa,female,"5-9"],x,x,x,x,...
          ......
          Population Cohorts[World,male,"95-99"],x,x,x,...
          Population Cohorts[World,male,"100+"],x,x,x,x,...
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
    logging.info("Sum up cleaned data by gender and age cohort")
    cleaned_data_by_region = cleaned_data.groupby(["un_region", "year"])["value"].sum()
    for region in regions:
        entry = {"parameter": f"Population[{region}]"}
        for year in range(1900, 2101):
            if year in year_avail:
                data_by_age = cleaned_data_by_region.loc[
                    region,
                    year,
                ]
                entry[year] = data_by_age
                del data_by_age
            else:
                entry[year] = ""
        restructured_data.append(entry)
        del entry
    del cleaned_data_by_region

    logging.info("Sum up cleaned data by age cohort")
    cleaned_data_no_age = cleaned_data.groupby(["un_region", "sex", "year"])[
        "value"
    ].sum()
    for region in regions:
        for sex in genders:
            entry = {"parameter": f"Population by Gender[{region},{sex}]"}
            for year in range(1900, 2101):
                if year in year_avail:
                    data_no_age = cleaned_data_no_age.loc[
                        region,
                        sex,
                        year,
                    ]
                    entry[year] = data_no_age
                    del data_no_age
                else:
                    entry[year] = ""
            restructured_data.append(entry)
            del entry
    del cleaned_data_no_age

    logging.info("Group cleaned data by all dimensions")
    cleaned_data_groups = cleaned_data.groupby(["un_region", "sex", "age"])
    for region in regions:
        for sex in genders:
            for age in age_cohorts:
                age = age.replace("--", "-")
                data_by_all = cleaned_data_groups.get_group(
                    (
                        region,
                        sex,
                        age,
                    ),
                ).reset_index(drop=True)

                entry = {"parameter": f"""Population Cohorts[{region},{sex},"{age}"]"""}

                for year in range(1900, 2101):
                    if year in year_avail:
                        try:
                            entry[year] = data_by_all.loc[
                                data_by_all["year"] == year, "value"
                            ].values[0]
                        except IndexError:
                            print(region, sex, age, year)
                            exit()
                    else:
                        entry[year] = ""

                restructured_data.append(entry)
                del entry

    del cleaned_data_groups

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_population = data_cleaning(
    raw_data=raw_population,
    raw_data_source=data_source,
    concordance=concordance_table,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

##################################################################################################
logging.info(f"Add SSP2 population data from IIASA based on the specified concordance")
logging.info(f"Load IIASA data")
iiasa_data_file = "1706548837040-ssp_basic_drivers_release_3.0_full.csv"
iiasa_data = pyam.IamDataFrame(
    data=Path("data_regionalization_raw")
    / f"version_{version}"
    / "iiasa"
    / iiasa_data_file
)
logging.info(f"Select population-related data from IIASA dataset")
iiasa_raw_data = (
    iiasa_data.filter(scenario="SSP2", variable="Population*", level=2)
    .timeseries()
    .reset_index()
)

logging.info(f"Load concordance between IIASA regions and FeliX regions")
concordance_file_iiasa = "iiasa_countries_to_5_un_regions.csv"
concordance_table_iiasa = pd.read_csv(
    path_concordance_folder / concordance_file_iiasa,
    encoding="utf-8",
)
concordance_table_iiasa = concordance_table_iiasa.dropna().reset_index(drop=True)
concordance_table_iiasa["un_region_code"] = concordance_table_iiasa[
    "un_region_code"
].astype("int")
logging.info(f"Finish reading concordance table for IIASA")

cleaned_data_iiasa = data_cleaning_iiasa(
    raw_data=iiasa_raw_data,
    concordance=concordance_table_iiasa,
)
##################################################################################################


# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
including_iiasa = True
if including_iiasa:
    restructured_population = data_restructure(
        cleaned_data=pd.concat(
            [
                cleaned_population.loc[cleaned_population["year"] < 2022],
                cleaned_data_iiasa,
            ]
        )
    )
    logging.info(f"Finish restructuring the cleaned data")

    logging.info(f"Start writing the restructured data")
    restructured_population.to_csv(
        path_clean_data_folder.joinpath(
            f"population_by_time_series_{data_source}_iiasa.csv"
        ),
        encoding="utf-8",
        index=False,
    )
    logging.info(f"Finish writing the restructured data")
    logging.info(f"The whole procedures of cleaning and restructuring data are done!")
else:
    restructured_population = data_restructure(
        cleaned_data=cleaned_population,
    )
    logging.info(f"Finish restructuring the cleaned data")

    logging.info(f"Start writing the restructured data")
    restructured_population.to_csv(
        path_clean_data_folder.joinpath(f"population_by_time_series_{data_source}.csv"),
        encoding="utf-8",
        index=False,
    )
    logging.info(f"Finish writing the restructured data")
    logging.info(f"The whole procedures of cleaning and restructuring data are done!")
