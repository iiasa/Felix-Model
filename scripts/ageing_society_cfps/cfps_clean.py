# -*- coding: utf-8 -*-
"""
Created: Tuesday 03 December 2024
Description: Scripts to match personal education and age of CFPS data
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

logging.info("Configure data source")
data_source = "cfps"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version
path_data_clean = data_home / "clean_data" / current_module / current_version

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify file name of input data")
file_name_cleaned_person = "person_info_full_cfps.csv"
cleaned_person = pd.read_csv(path_data_clean / file_name_cleaned_person)

logging.info("Configure data years")
years = ["2014", "2016", "2018", "2020"]

for year in years:
    # load family consumption data of year
    logging.info(f"Load family consumption data of survey year {year}")
    for file_path in (path_data_raw / f"cfps_{year}").glob("*"):
        if "famecon" in str(file_path):
            raw_famecon = pd.read_stata(file_path, convert_categoricals=False)

    logging.info(f"Specific maximal number of person in one household for {year}")
    if year == "2014":
        max_fam_num = 17
    elif year == "2016":
        max_fam_num = 19
    elif year == "2018":
        max_fam_num = 21
    elif year == "2020":
        max_fam_num = 15

    raw_famecon_melted = pd.melt(
        raw_famecon,
        id_vars=[
            f"fid{year[-2:]}",
            f"provcd{year[-2:]}",
            "daily",
            "dress",
            "eec",
            "food",
            "house",
            "med",
            "trco",
            "pce",
            "other",
        ],
        value_vars=[f"pid_a_{p_ind}" for p_ind in range(1, max_fam_num + 1)],
        var_name="pid_column",
        value_name="pid",
    )

    logging.info(f"Merge personal information with each person in the household")
    raw_merged = pd.merge(
        raw_famecon_melted,
        cleaned_person[["pid", f"age_in_{year}", f"cfps{year}edu"]],
        on="pid",
        how="left",
    )

    # Remove duplicate rows according to total family consumption
    raw_merged = raw_merged.sort_values(by="pce", ascending=False).drop_duplicates(
        subset="pid", keep="first"
    )

    # Reshape merged data back to wide format
    raw_merged_structured = raw_merged.pivot(
        index=[
            f"fid{year[-2:]}",
            f"provcd{year[-2:]}",
            "daily",
            "dress",
            "eec",
            "food",
            "house",
            "med",
            "trco",
            "pce",
            "other",
        ],
        columns="pid_column",
        # with education
        values=["pid", f"age_in_{year}", f"cfps{year}edu"],
    ).reset_index()
    raw_merged_structured = raw_merged_structured.dropna(how="all")

    # Flatten the MultiIndex columns
    raw_merged_structured.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0]
        for col in raw_merged_structured.columns
    ]

    # save data
    logging.info("Save cleaned data")
    file_name = f"cfps_household_data_{year}.csv"
    raw_merged_structured.to_csv(path_data_clean / file_name, index=False)
