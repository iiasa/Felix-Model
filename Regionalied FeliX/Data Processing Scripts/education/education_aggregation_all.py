# -*- coding: utf-8 -*-
"""
Created: Tue 27 Feburary 2024
Description: Scripts to aggregate population by educational level data by region from Wittgenstein to 5 FeliX regions
Scope: FeliX model regionalization, module education 
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

import pandas as pd

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the data source of education
# data sources are wittgensteinwittgenstein, world_bank, unpd
data_source = "wittgenstein"
download_method = "manually"
variable = "pop_by_edu"

# read config.yaml file
yaml_dir = Path("scripts/education/config.yaml")
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
        dataset["datasource"] == data_source
        and dataset["variable"] == variable
        and dataset["download_via"] == download_method
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
# age_cohorts = data_info["dimension"]["age"][3:]
age_cohorts = data_info["dimension"]["age"]
edu_levels = data_info["dimension"]["edu_level"]
logging.info("Extracted dimensions of regions, genders, and ages")

# Read raw data
logging.info(f"Read raw data")
raw_education = pd.DataFrame()
if raw_data_info["datasource"] == "wittgenstein":
    for raw_data_file in raw_data_files:
        logging.info(f"Data input: {path_raw_data_folder / raw_data_file}")
        if download_method == "manually":
            raw_education_ = pd.read_csv(
                path_raw_data_folder / raw_data_file,
                skiprows=8,
                encoding="utf-8",
            )
            raw_education_.columns = raw_education_.columns.str.lower()
            raw_education = pd.concat(
                [raw_education, raw_education_],
                ignore_index=True,
            )
            del raw_education_
elif raw_data_info["datasource"] == "world_bank":
    logging.info("2 data source is world_bank")
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

logging.info("Start reading condordance table of educational level")
concordance_file_edu = "wittgenstein_education_levels_to_felix_levels.csv"
concordance_table_edu = pd.read_csv(
    path_concordance_folder / concordance_file_edu,
    encoding="utf-8",
)
concordance_table_edu = concordance_table_edu.dropna()
concordance_table_edu["three_edu_code"] = concordance_table_edu[
    "three_edu_code"
].astype("int")
logging.info(f"Finish reading concordance table of educational level")


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
            concordance_edu=kwargs["concordance_edu"],
        )
    elif raw_data_source == "world_bank":
        cleaned_data = data_cleaning_world_bank(
            raw_data=raw_data,
            concordance=concordance,
            concordance_edu=kwargs["concordance_edu"],
        )
    elif raw_data_source == "unpd":
        cleaned_data = data_cleaning_unpd(
            raw_data=raw_data,
            concordance=concordance,
            concordance_edu=kwargs["concordance_edu"],
        )

    return cleaned_data


def data_cleaning_wittgenstein(
    raw_data: pd.DataFrame,
    concordance: pd.DataFrame,
    **kwargs,
):
    """
    To clean raw data from wittgenstein to a more readable format

    Parameter
    ---------
    raw_data: pd.DataFrame
        Raw data from wittgenstein

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
    concordance_edu = kwargs["concordance_edu"]

    if download_method == "manually":
        logging.info("First merge data based on locations")
        raw_data_merge = pd.merge(
            raw_data,
            concordance[["location", "un_region"]],
            left_on="area",
            right_on="location",
        )
        logging.info("Second merge data based on education levels")
        raw_data_merge = pd.merge(
            raw_data_merge,
            concordance_edu[["education", "felix_edu"]],
            on="education",
        )
        raw_data_merge["sex"] = [value.lower() for value in raw_data_merge["sex"]]

    years = np.unique(raw_data_merge["year"])
    raw_data_groups = raw_data_merge.groupby(
        ["un_region", "sex", "age", "year", "felix_edu"]
    )
    cleaned_data = []
    for region in regions:
        for sex in genders:
            for age in age_cohorts:
                for year in years:
                    for edu_level in edu_levels:
                        raw_data_region_year = (
                            raw_data_groups.get_group(
                                (
                                    region,
                                    sex,
                                    age,
                                    year,
                                    edu_level,
                                )
                            )["population"].sum()
                            * 1000
                        )  # convert to person

                        entry = {
                            "un_region": region,
                            "sex": sex,
                            "age": age,
                            "education": edu_level,
                            "year": year,
                            "value": raw_data_region_year,
                            "unit": "person",
                        }
                        cleaned_data.append(entry)
                        del entry, raw_data_region_year, edu_level

    cleaned_data = pd.DataFrame(cleaned_data)
    cleaned_data = cleaned_data.astype({"year": "int"})

    return cleaned_data


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
    print("this is the data_cleaning_unpd")


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
    concordance_edu = kwargs["concordance_edu"]
    logging.info("First merge data based on locations")
    raw_data_merge = pd.merge(
        raw_data,
        concordance[["location", "un_region"]],
        left_on="region",
        right_on="location",
    )
    logging.info("Second merge data based on education levels")
    raw_data_merge = pd.merge(
        raw_data_merge,
        concordance_edu[["education_iiasa", "felix_edu"]].rename(
            columns={"education_iiasa": "education"}
        ),
        on="education",
    )
    raw_data_merge["sex"] = [value.lower() for value in raw_data_merge["sex"]]

    raw_data_groups = raw_data_merge.groupby(["un_region", "sex", "age", "felix_edu"])
    cleaned_data = []
    for region in regions:
        for sex in genders:
            for age in age_cohorts:
                for edu_level in edu_levels:
                    if edu_level != "total":
                        try:
                            raw_data_region_year = raw_data_groups.get_group(
                                (
                                    region,
                                    sex,
                                    f"Age {age.replace('--','-')}",
                                    edu_level,
                                )
                            )
                        except KeyError:
                            raw_data_region_year = pd.DataFrame()
                            for column_name in range(2025, 2101, 5):
                                entry = {
                                    "un_region": region,
                                    "sex": sex,
                                    "age": age,
                                    "education": edu_level,
                                    "year": column_name,
                                    "value": 0,
                                    "unit": "person",
                                }
                                cleaned_data.append(entry)
                                del entry, column_name

                        if not raw_data_region_year.empty:
                            for column_name in raw_data_region_year.columns:
                                if type(column_name) == int and column_name > 2020:
                                    entry = {
                                        "un_region": region,
                                        "sex": sex,
                                        "age": age,
                                        "education": edu_level,
                                        "year": column_name,
                                        "value": raw_data_region_year[column_name].sum()
                                        * 1000000,  # convert to person
                                        "unit": "person",
                                    }
                                    cleaned_data.append(entry)
                                    del entry, column_name
                        del raw_data_region_year, edu_level

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
          Population with No or Incomplete Education[Africa,female,"15-19"],x,x,x,...
          Population with No or Incomplete Education[Africa,female,"20-24"],x,x,x,...
          ......
          Total Population with No or Incomplete Education by Gender[Africa,female],x,x,x,...
          Total Population with No or Incomplete Education by Gender[Africa,male],x,x,x,...
          Total Population with No or Incomplete Education[Africa],x,x,x,...
          ......
          Tertiary Education Graduates by Gender[WestEu,male],x,x,x,...
          Total Tertiary Education Graduates[WestEu],x,x,x,x,...
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
    year_avail = np.unique(cleaned_data["year"])
    logging.info("Distinguish data by educational level and region")
    cleaned_data_groups = cleaned_data.groupby(["education", "un_region"])
    restructured_data = []
    for edu_level in edu_levels:
        if edu_level != "total":
            for region in regions:
                cleaned_data_edu_region = cleaned_data_groups.get_group(
                    (edu_level, region)
                )

                logging.info(f"Generate total population by region and education level")
                entry = {"parameter": f"Total {parameter_map[edu_level]}[{region}]"}
                for year in range(1900, 2101):
                    if year in year_avail:
                        data_by_year = cleaned_data_edu_region.loc[
                            cleaned_data_edu_region["year"] == year
                        ]["value"].sum()
                        entry[year] = data_by_year
                        del data_by_year
                    else:
                        entry[year] = ""
                restructured_data.append(entry)
                del entry

                logging.info(
                    f"Generate total population by region, education level, and sex"
                )
                cleaned_data_edu_region_groups = cleaned_data_edu_region.groupby("sex")
                for sex in genders:
                    cleaned_data_edu_region_sex = (
                        cleaned_data_edu_region_groups.get_group(sex)
                    )
                    if edu_level == "noEd":
                        entry = {
                            "parameter": f"Total {parameter_map[edu_level]} by Gender[{region},{sex}]"
                        }
                    else:
                        entry = {
                            "parameter": f"{parameter_map[edu_level]} by Gender[{region},{sex}]"
                        }
                    for year in range(1900, 2101):
                        if year in year_avail:
                            data_by_year = cleaned_data_edu_region_sex.loc[
                                cleaned_data_edu_region_sex["year"] == year
                            ]["value"].sum()

                            entry[year] = data_by_year
                            del data_by_year
                        else:
                            entry[year] = ""
                    restructured_data.append(entry)
                    del entry, cleaned_data_edu_region_sex
                del cleaned_data_edu_region_groups

                logging.info(
                    f"Generate total population by region, education level, sex, and age"
                )
                cleaned_data_edu_region_groups = cleaned_data_edu_region.groupby(
                    ["sex", "age"]
                )
                for sex in genders:
                    for age in age_cohorts:
                        cleaned_data_edu_region_sex_age = (
                            cleaned_data_edu_region_groups.get_group((sex, age))
                        )

                        entry = {
                            "parameter": f"""{parameter_map[edu_level]}[{region},{sex},"{age.replace('--','-')}"]"""
                        }
                        for year in range(1900, 2101):
                            if year in year_avail:
                                data_by_year = cleaned_data_edu_region_sex_age.loc[
                                    cleaned_data_edu_region_sex_age["year"] == year
                                ]["value"].sum()

                                entry[year] = data_by_year
                                del data_by_year
                            else:
                                entry[year] = ""
                        restructured_data.append(entry)
                        del entry, cleaned_data_edu_region_sex_age

    restructured_data = pd.DataFrame(restructured_data)

    return restructured_data


# Start cleaning raw data
logging.info(
    f"Start cleaning the raw data from {data_source} based on the specified concordance"
)
cleaned_education = data_cleaning(
    raw_data=raw_education,
    raw_data_source=data_source,
    concordance=concordance_table,
    concordance_edu=concordance_table_edu,
)
logging.info(f"Finish cleaning the raw data from {data_source}")

# ##################################################################################################
logging.info(f"Add SSP2 education data from IIASA based on the specified concordance")
logging.info(f"Load IIASA data")
iiasa_data_file = "1706548837040-ssp_basic_drivers_release_3.0_full.csv"
iiasa_data = pyam.IamDataFrame(
    data=Path("data_regionalization_raw")
    / f"version_{version}"
    / "iiasa"
    / iiasa_data_file
)

logging.info(f"Select education-related data from IIASA dataset")
# population older than 15 only
iiasa_raw_data = (
    iiasa_data.filter(scenario="SSP2", variable="Population*", level=3)
    .timeseries()
    .reset_index()
)
iiasa_raw_data[["population", "sex", "age", "education"]] = iiasa_raw_data[
    "variable"
].str.split(
    "|",
    expand=True,
)
iiasa_raw_data.drop(
    columns=["variable", "population"],
    axis=1,
    inplace=True,
)

# population younger than 15
variable_iiasa = []
for age in age_cohorts[:3]:
    age = age.replace("--", "-")
    variable_iiasa.append(f"Population|Female|Age {age}")
    variable_iiasa.append(f"Population|Male|Age {age}")
    del age

iiasa_raw_data_0_15 = pd.DataFrame()
for variable in variable_iiasa:
    iiasa_raw_0_15 = (
        iiasa_data.filter(scenario="SSP2", variable=variable).timeseries().reset_index()
    )
    iiasa_raw_data_0_15 = pd.concat(
        [
            iiasa_raw_data_0_15,
            iiasa_raw_0_15,
        ]
    )
    del iiasa_raw_0_15
iiasa_raw_data_0_15[["population", "sex", "age"]] = iiasa_raw_data_0_15[
    "variable"
].str.split(
    "|",
    expand=True,
)
iiasa_raw_data_0_15.drop(
    columns=["variable", "population"],
    axis=1,
    inplace=True,
)
iiasa_raw_data_0_15["education"] = "Under 15"

iiasa_raw_data = pd.concat(
    [
        iiasa_raw_data,
        iiasa_raw_data_0_15,
    ]
)
del iiasa_raw_data_0_15

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
    concordance_edu=concordance_table_edu,
)
# ##################################################################################################

# Start restructuring cleaned data, which will be used as historic data
logging.info("Start restructing the cleaned data based on the specified concordance")
parameter_map = {
    "noEd": "Population with No or Incomplete Education",
    "primary": "Primary Education Graduates",
    "secondary": "Secondary Education Graduates",
    "tertiary": "Tertiary Education Graduates",
}

# # without IIASA data
# restructured_education = data_restructure(
#     cleaned_data=cleaned_education,
# )
# logging.info(f"Finish restructuring the cleaned data")


# logging.info(f"Start writing the restructured data")
# restructured_education.to_csv(
#     path_clean_data_folder.joinpath(f"education_by_time_series_{data_source}.csv"),
#     encoding="utf-8",
#     index=False,
# )
# logging.info(f"Finish writing the restructured data")
# logging.info(f"The whole procedures of cleaning and restructuring data are done!")


# with IIASA data
restructured_education = data_restructure(
    cleaned_data=pd.concat(
        [
            cleaned_education.loc[cleaned_education["year"] < 2025],
            cleaned_data_iiasa,
        ]
    )
)
logging.info(f"Finish restructuring the cleaned data")


logging.info(f"Start writing the restructured data")
restructured_education.to_csv(
    path_clean_data_folder.joinpath(
        f"education_by_time_series_{data_source}_iiasa.csv"
    ),
    encoding="utf-8",
    index=False,
)
logging.info(f"Finish writing the restructured data")
logging.info(f"The whole procedures of cleaning and restructuring data are done!")
