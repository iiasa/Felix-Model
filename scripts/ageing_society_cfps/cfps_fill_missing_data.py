# -*- coding: utf-8 -*-
"""
Created: Tuesday 03 December 2024
Description: Scripts to fill missing data, matching personal education and age of CFPS data
Scope: Ageing society project, module ageing_society
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
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv("CURRENT_VERSION")

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Specify the log message format
    datefmt="%Y-%m-%d %H:%M:%S",  # Specify the date format
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("app.log"),  # Log to a file
    ],
)

logging.info("Configure module")
current_module = "ageing_society"

logging.info("Specify data source")
data_source = "cfps"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Configure data years")
years = ["2014", "2016", "2018", "2020"]

logging.info("Read available data")
raw_person = pd.DataFrame()
for year in years:
    for file_path in (path_data_raw / f"cfps_{year}").glob("*"):
        if "person" in str(file_path) or "adult" in str(file_path):
            raw_adult = pd.read_stata(file_path, convert_categoricals=False)
        elif "child" in str(file_path):
            raw_child = pd.read_stata(file_path, convert_categoricals=False)
        elif "famecon" in str(file_path):
            raw_famecon = pd.read_stata(file_path, convert_categoricals=False)

    try:
        raw_person_ = pd.concat(
            [
                raw_adult[["pid", "age", f"cfps{year}edu"]],
                raw_child[["pid", "age", f"cfps{year}edu"]],
            ],
            ignore_index=True,
        ).rename(columns={f"age": f"age_in_{year}"})
    except KeyError:
        try:
            raw_person_ = pd.concat(
                [
                    raw_adult[["pid", f"cfps{year}_age", f"cfps{year}edu"]],
                    raw_child[["pid", f"cfps{year}_age", f"cfps{year}edu"]],
                ],
                ignore_index=True,
            ).rename(columns={f"cfps{year}_age": f"age_in_{year}"})
        except KeyError:
            # with education
            raw_person_ = pd.concat(
                [
                    raw_adult[["pid", "cfps_age", f"cfps{year}edu"]],
                    raw_child[["pid", "cfps_age", f"cfps{year}edu"]],
                ],
                ignore_index=True,
            ).rename(columns={"cfps_age": f"age_in_{year}"})

    # process missing age data
    logging.info("Process missing age data")
    for year_ in years:
        if year_ != year:
            raw_person_[f"age_in_{year_}"] = (
                raw_person_[f"age_in_{year}"] + int(year_) - int(year)
            ).clip(lower=0)

    raw_person = pd.concat([raw_person, raw_person_], ignore_index=True)
    del year

raw_person = raw_person.groupby("pid", as_index=False).max()
del raw_adult, raw_child

logging.info("Process missing education data")
edu_columns = [f"cfps{year}edu" for year in years]
# Step 1, replace all incorrectly reported data with NaN, incorrectly reported data including:
# -9: Missing Data; -1: I Don't Know; 9: No Need for Education
# Then count the availability of educational data
raw_person[edu_columns] = raw_person[edu_columns].replace([-9, -1, 9], np.nan)
raw_person["count_avai"] = raw_person[edu_columns].apply(
    lambda row: (pd.to_numeric(row, errors="coerce") >= 0).sum(), axis=1
)


def filling_edu_base_one(raw_person_education: pd.DataFrame):
    """
    This is the function to filling missing education data based on only one year data availability

    Parameter:
    raw_person_education: pd.DataFrame
        Filtered personal data with age and educational level in survey years,
        but only one year educational data available

    return:
        cleaned_person_education: pd.DataFrame
    """
    for row_index in range(len(raw_person_education)):
        row_edu_ = raw_person_education.iloc[row_index, :]

        # filling based on the maximal educational level
        if row_edu_["edu_avai"] == 9:
            # education code 9 means no need for education?
            raw_person_education.loc[row_index, edu_columns] = 0

        elif row_edu_["edu_avai"] == 1:
            # the available educational level is illiterate
            # the next education level could be primary,
            # also may be illiterate during the whole lifetime
            if row_edu_[f"age_in_{years[0]}"] >= 10:
                # assuming that if age in the first year older than 10,
                # and the available educational level from 10 is illiterate
                # the person is illiterate during the whole life
                raw_person_education.loc[row_index, edu_columns] = row_edu_["edu_avai"]
            else:
                # Find which year has the educational level avaialble
                for year in years:
                    if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                        year_ = int(year)

                # Filling in missing data, depending on age
                for year in years:
                    if int(year) > year_:
                        if row_edu_[f"age_in_{year}"] > 5:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) + 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )
                    else:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                            row_edu_["edu_avai"]
                        )

        elif row_edu_["edu_avai"] == 2:
            # the available educational level is primary
            # the next education level could be secondary,
            # also may be primary during the whole lifetime
            if row_edu_[f"age_in_{years[0]}"] >= 16:
                # assuming that if age in the first year older than 16,
                # and the available educational level is primary
                # the person is primary during the whole life
                raw_person_education.loc[row_index, edu_columns] = row_edu_["edu_avai"]
            else:
                # Find which year has the educational level avaialble
                for year in years:
                    if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                        year_ = int(year)

                # Filling in missing data, depending on age
                for year in years:
                    if int(year) > year_:
                        if row_edu_[f"age_in_{year}"] > 12:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) + 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )
                    else:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                            row_edu_["edu_avai"]
                        )

        elif row_edu_["edu_avai"] == 3:
            # the available educational level is middle school
            # the next education level could be high school depending on age,
            # the previous education level could be primary school depending on age
            # also may be middle school during the whole lifetime
            if row_edu_[f"age_in_{years[0]}"] > 19:
                # assuming that if age in the first year older than 19,
                # and the available educational level is middle school
                # the person is middle school during the whole life
                raw_person_education.loc[row_index, edu_columns] = row_edu_["edu_avai"]
            else:
                # Find which year has the educational level avaialble
                for year in years:
                    if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                        year_ = int(year)

                # Filling in missing data, depending on age
                for year in years:
                    if int(year) > year_:
                        if row_edu_[f"age_in_{year}"] > 15:
                            # assuming that if the age larger than 15, the person is enrolled into next education level
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) + 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )
                    else:
                        if row_edu_[f"age_in_{year}"] < 6:
                            # person younger than 6 years old mostly not enrolled into primary
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) - 2
                        elif row_edu_[f"age_in_{year}"] < 13:
                            # person between 6-13 years old mostly in primary school
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) - 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )

        elif row_edu_["edu_avai"] == 4:
            # the available educational level is high school
            # the next education level could be tertiary depending on age,
            # the previous education level could be middle school depending on age
            # also may be high school during the whole lifetime

            if row_edu_[f"age_in_{years[0]}"] > 22:
                # assuming that if age in the first survey year older than 22,
                # and the available educational level is high school
                # the person is high school during the whole life
                raw_person_education.loc[row_index, edu_columns] = row_edu_["edu_avai"]
            else:
                # Find which year has the educational level avaialble
                for year in years:
                    if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                        year_ = int(year)

                # Filling in missing data, depending on age
                for year in years:
                    if int(year) > year_:
                        if row_edu_[f"age_in_{year}"] > 18:
                            # assuming that if the age larger than 18, the person is enrolled into next education level
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) + 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )
                    else:
                        if row_edu_[f"age_in_{year}"] < 12:
                            # person younger than 12 years old mostly in primary school
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) - 2
                        elif row_edu_[f"age_in_{year}"] < 16:
                            # person between 12-16 years old mostly in middle school
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            ) - 1
                        else:
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                row_edu_["edu_avai"]
                            )

        elif row_edu_["edu_avai"] in [5, 6]:
            # the available education level is tertiary,
            # the previous educational level is secondary
            # Find which year has the educational level avaialble
            for year in years:
                if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                    year_ = int(year)

            # Filling in missing data, depending on age
            for year in years:
                if int(year) > year_:
                    raw_person_education.loc[row_index, f"cfps{year}edu"] = row_edu_[
                        "edu_avai"
                    ]
                else:
                    if row_edu_[f"age_in_{year}"] <= 19:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = 4
                    else:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                            row_edu_["edu_avai"]
                        )

        elif row_edu_["edu_avai"] in [7, 8]:
            # the available educational level is tertiary
            # the previous educational level is also teritiary
            # Find which year has the educational level avaialble
            for year in years:
                if row_edu_[f"cfps{year}edu"] == row_edu_["edu_avai"]:
                    year_ = int(year)

            # Filling in missing data, depending on age
            for year in years:
                if int(year) > year_:
                    raw_person_education.loc[row_index, f"cfps{year}edu"] = row_edu_[
                        "edu_avai"
                    ]
                else:
                    if row_edu_[f"age_in_{year}"] <= 29:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                            row_edu_["edu_avai"]
                        ) - 1
                    else:
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                            row_edu_["edu_avai"]
                        )

        elif (
            row_edu_[f"age_in_{years[0]}"] > 29 or row_edu_[f"age_in_{years[-1]}"] < 14
        ):
            # if age in the first year older than 29 or age in the last year younger than 14
            raw_person_education.loc[row_index, edu_columns] = row_edu_["edu_avai"]

    return raw_person_education


def filling_edu_base_two(raw_person_education: pd.DataFrame):
    """
    This is the function to filling missing education data based on two year data availability

    Parameter:
    raw_person_education: pd.DataFrame
        Filtered personal data with age and educational level in survey years,
        two year educational data available

    return:
        cleaned_person_education: pd.DataFrame
    """
    for row_index in range(len(raw_person_education)):
        row_edu_ = raw_person_education.iloc[row_index, :]

        # Find the maximal and minimal educational level
        edu_min = pd.to_numeric(row_edu_[edu_columns], errors="coerce").min()
        edu_max = pd.to_numeric(row_edu_[edu_columns], errors="coerce").max()

        for year in years:
            if np.isnan(row_edu_[f"cfps{year}edu"]):
                # filling missing educational data based on age
                if year in [years[0], years[-1]]:
                    # educational level in the first or last survey year is missing
                    if row_edu_[f"age_in_{year}"] > 29:
                        # assume that if the person is older than 29 years old
                        # the missing educational data is the person's higher educational level
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = edu_max
                    elif row_edu_[f"age_in_{year}"] < 6:
                        # assume that if the person is younger than 6 years old,
                        # the missing educational data is illiterate (code 1)
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = 1
                    else:
                        if year == years[0]:
                            if edu_min >= 3:
                                if row_edu_[f"age_in_{year}"] <= 12:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_min - 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_min
                            else:
                                # assume that the person has the minimal educational level
                                # if educational data in the first survey year is missing
                                raw_person_education.loc[
                                    row_index, f"cfps{year}edu"
                                ] = edu_min
                        else:
                            if row_edu_[f"age_in_{year}"] >= 18:
                                if edu_max == 4:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_max + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                            elif row_edu_[f"age_in_{year}"] > 15:
                                if edu_max in [2, 3, 4]:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_max + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                            elif row_edu_[f"age_in_{year}"] > 12:
                                if edu_max in [1, 2, 3]:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_max + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                            elif row_edu_[f"age_in_{year}"] >= 6:
                                if edu_max == 1:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_max + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                else:
                    # educational level in the middle survey year is missing
                    if row_edu_[f"age_in_{year}"] > 29:
                        # assume that if the person is older than 29 years old
                        # the missing educational data is the person's higher educational level
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = edu_max
                    elif row_edu_[f"age_in_{year}"] < 6:
                        # assume that if the person is younger than 6 years old,
                        # the missing educational data is illiterate (code 1)
                        raw_person_education.loc[row_index, f"cfps{year}edu"] = 1
                    else:
                        if (
                            row_edu_[f"cfps{int(year)+2}edu"] == edu_min
                            or row_edu_[f"cfps{years[-1]}edu"] == edu_min
                        ):
                            raw_person_education.loc[row_index, f"cfps{year}edu"] = (
                                edu_min
                            )
                        else:
                            if (
                                row_edu_[f"cfps{int(year)-2}edu"] == edu_max
                                or row_edu_[f"cfps{years[0]}edu"] == edu_max
                            ):
                                if row_edu_[f"age_in_{year}"] in [6, 7, 8] + list(
                                    range(12, 21)
                                ):
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_max + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                            else:
                                if edu_min == edu_max:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max
                                elif edu_max - edu_min > 1:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = (edu_min + 1)
                                else:
                                    raw_person_education.loc[
                                        row_index, f"cfps{year}edu"
                                    ] = edu_max

    return raw_person_education


def filling_edu_base_three(raw_person_education: pd.DataFrame):
    """
    This is the function to filling missing education data based on three-year data availability

    Parameter:
    raw_person_education: pd.DataFrame
        Filtered personal data with age and educational level in survey years,
        with three-year educational data available

    return:
        cleaned_person_education: pd.DataFrame
    """
    for row_index in range(len(raw_person_education)):
        row_edu_ = raw_person_education.iloc[row_index, :]

        # Find in which year the educational level data misses
        year_ = 0
        for year in years:
            if np.isnan(row_edu_[f"cfps{year}edu"]):
                year_ = int(year)

        # filling missing educational data based on age
        if year_ == int(years[0]):
            # Educational level in the first year is missing
            # filling it based on the age and the next educational level
            if row_edu_[f"age_in_{year_}"] < 6:
                # assume that people enroll into primary school at 6, the earliest
                if row_edu_[f"cfps{year_+2}edu"] in [1, 2]:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is illiterate or primary
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = 1
                else:
                    raise KeyError("The person is in middle school before 8 years old")
            elif row_edu_[f"age_in_{year_}"] <= 12:
                # assume that people graduate from primary school at 12, the earliest
                if row_edu_[f"cfps{year_+2}edu"] == 1:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is illiterate
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_+2}edu"
                    ]
                elif row_edu_[f"cfps{year_+2}edu"] == 2:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is primary school
                    if row_edu_[f"age_in_{year_}"] < 6:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        ) - 1
                    else:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        )
                elif row_edu_[f"cfps{year_+2}edu"] == 3:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is middle school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_+2}edu"] - 1
                    )
                else:
                    raise KeyError("The person is in high school before 14 years old")
            elif row_edu_[f"age_in_{year_}"] <= 15:
                # assume that people graduate from middle school at 15, the earliest
                if row_edu_[f"cfps{year_+2}edu"] in [1, 2]:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is illiterate or primary school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_+2}edu"
                    ]
                elif row_edu_[f"cfps{year_+2}edu"] == 3:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is middle school
                    if row_edu_[f"age_in_{year_}"] < 12:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        ) - 1
                    else:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        )
                elif row_edu_[f"cfps{year_+2}edu"] == 4:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is high school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_+2}edu"] - 1
                    )
                else:
                    raise KeyError(
                        "The person is in teritary school before 17 years old"
                    )
            elif row_edu_[f"age_in_{year_}"] <= 18:
                # assume that people graduate from high school at 18, the earliest
                if row_edu_[f"cfps{year_+2}edu"] in [1, 2, 3]:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is illiterate or primary or middle school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_+2}edu"
                    ]
                elif row_edu_[f"cfps{year_+2}edu"] == 4:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is high school
                    if row_edu_[f"age_in_{year_}"] < 15:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"] - 1
                        )
                    else:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        )
                elif row_edu_[f"cfps{year_+2}edu"] == 5:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is teritary school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_+2}edu"] - 1
                    )
                else:
                    # The person is in teritary school before 20 years old"
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = 4
            elif row_edu_[f"age_in_{year_}"] <= 21:
                # assume that people graduate from teritary school at 21, the earliest
                if row_edu_[f"cfps{year_+2}edu"] in [1, 2, 3, 4]:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is illiterate or primary or middle or high school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_+2}edu"
                    ]
                elif row_edu_[f"cfps{year_+2}edu"] in [5, 6]:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is three-year or four-year college
                    if row_edu_[f"age_in_{year_}"] < 18:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        ) - 1
                    else:
                        raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                            row_edu_[f"cfps{year_+2}edu"]
                        )
                elif row_edu_[f"cfps{year_+2}edu"] == 7:
                    # survey is conducted every 2 years, so the next survey year is year_+2
                    # the educational level of the peron in the next survey year is teritary school
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_+2}edu"] - 1
                    )
                else:
                    raise KeyError(
                        "The person is studying for doctor before 22 years old"
                    )
            else:
                raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                    f"cfps{year_+2}edu"
                ]
        elif year_ == int(years[-1]):
            # Educational level in the last year is missing
            # filling it based on the age and the previous educational level
            if row_edu_[f"age_in_{year_}"] > 20 or row_edu_[f"age_in_{year_}"] < 4:
                # assume that people enroll into tertiary education at 18, the earliest;
                # when person older than 20 years old, they are stay with the educational level they are at 18
                # In addition, if the person is younger than 4 years old, the educational level doesn't change
                raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                    f"cfps{year_-2}edu"
                ]
            elif row_edu_[f"age_in_{year_}"] >= 10:
                if row_edu_[f"cfps{year_-2}edu"] in [1, 2, 3, 4]:
                    # survey is conducted every 2 years, so the previous survey year is year_-2
                    # if the previous educational level is middle or high school,
                    # assume the person enroll to the next level of education
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_-2}edu"] + 1
                    )
                else:
                    # assume the person keep the previous educational level
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_-2}edu"
                    ]
            elif row_edu_[f"age_in_{year_}"] >= 4:
                if row_edu_[f"cfps{year_-2}edu"] == 1:
                    # survey is conducted every 2 years, so the previous survey year is year_-2
                    # if the previous educational level is middle or high school,
                    # assume the person enroll to the next level of education
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = (
                        row_edu_[f"cfps{year_-2}edu"] + 1
                    )
                else:
                    # assume the person keep the previous educational level
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_-2}edu"
                    ]
        else:
            # Educational level in one of the middle survey year is missing
            # filling it based on the age, the previous and the next educational level
            if row_edu_[f"cfps{year_-2}edu"] == row_edu_[f"cfps{year_+2}edu"]:
                # the previous and the next educational level is same
                raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                    f"cfps{year_-2}edu"
                ]
            else:
                if row_edu_[f"age_in_{year_}"] < 6 or row_edu_[f"age_in_{year_}"] in [
                    10,
                    11,
                    13,
                    14,
                    16,
                    17,
                    20,
                    21,
                ]:
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_-2}edu"
                    ]
                else:
                    raw_person_education.loc[row_index, f"cfps{year_}edu"] = row_edu_[
                        f"cfps{year_+2}edu"
                    ]

    return raw_person_education


# Step 2, filling education data in the four years
raw_person_groups = raw_person.groupby("count_avai")
raw_person_edu_adj = pd.DataFrame()
for count_num in range(5):
    raw_person_edu = raw_person_groups.get_group(count_num).reset_index(drop=True)

    if count_num == 0:
        logging.info("No education data available in all years")
        # no education data avaliable in all four years,
        # put a 0 value
        raw_person_edu.loc[:, edu_columns] = 0
    elif count_num == 1:
        logging.info("Only one year with education data")
        # only one year with education data
        raw_person_edu["edu_avai"] = raw_person_edu[edu_columns].apply(
            lambda row: pd.to_numeric(row, errors="coerce").max(), axis=1
        )

        raw_person_edu = filling_edu_base_one(raw_person_education=raw_person_edu)
    elif count_num == 2:
        logging.info("Two year data available")
        raw_person_edu = filling_edu_base_two(raw_person_education=raw_person_edu)
    elif count_num == 3:
        logging.info("Three year data available")
        raw_person_edu = filling_edu_base_three(raw_person_education=raw_person_edu)

    raw_person_edu_adj = pd.concat(
        [raw_person_edu_adj, raw_person_edu], ignore_index=True
    )
    del raw_person_edu

# save filled data
logging.info("Save filled data")
file_name = "cfps_person_info_full.csv"
raw_person_edu_adj.to_csv(path_data_clean / file_name, index=False)
