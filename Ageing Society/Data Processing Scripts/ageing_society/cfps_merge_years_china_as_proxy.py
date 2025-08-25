# -*- coding: utf-8 -*-
"""
Created: Tue 25 Mar 2025
Description: Scripts to use proxy data for associated countries based on GDP per capita
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""
import datetime
import logging
import json
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_AGEING_SOCIETY")
current_version_regionalization = os.getenv(f"CURRENT_VERSION_FELIX_REGIONALIZATION")

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
country_proxy = "china"
country = "china_proxy_countries"

logging.info("Configure paths")
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

logging.info("Load the proxy countries, in unpd names")
try:
    proxy_countries_unpd = pd.read_csv(path_data_clean / "china_proxy_countries.csv")
except FileNotFoundError:
    logging.warning("Countries proxied by CFPS data were not found")
proxy_countries_unpd = list(proxy_countries_unpd["region_api"]) + ["China"]

######################################################################################
# PROCESSING HOUSEHOLD FINAL CONSUMPTION PER CAPITA DATA
######################################################################################
logging.info("Process household final consumption per capita data")
exteral_project = "felix_regionalization"
exteral_module = "gdp"
path_data_raw = (
    data_home / "raw_data" / exteral_project / current_version_regionalization
)

logging.info("Load concordance table")
concordance_file = "unpd_regions_to_world_bank_countries.csv"
concordance_table = pd.read_csv(path_data_raw / "concordance" / concordance_file)
country_name_mapping = concordance_table.set_index("region_api")[
    "location_world_bank"
].to_dict()
proxy_countries_worldbank = [
    country_name_mapping[country_name] for country_name in proxy_countries_unpd
]

raw_data_file = "NE_CON_PRVT_PC_KD_fact.json"
with open(path_data_raw / exteral_module / raw_data_file) as fact_file:
    raw_hh_exp_per_capita_ = json.load(fact_file)
raw_hh_exp_per_capita = []
for data_point in raw_hh_exp_per_capita_:
    country_ = data_point["country"]
    if country_ in proxy_countries_worldbank:
        if data_point["value"] == "":
            data_point["value"] = np.nan
        raw_hh_exp_per_capita.append(data_point)
        del data_point
raw_hh_exp_per_capita = pd.DataFrame(raw_hh_exp_per_capita)
del raw_hh_exp_per_capita_

logging.info("Load the cleaned CFPS data")
try:
    cleaned_exp_chn = pd.read_csv(
        path_data_clean / "cfps_household_data_all_years_in_2005_usd.csv"
    )
except FileNotFoundError:
    logging.warning("No cleaned CFPS data was found")

cleaned_exp_chn_groups = cleaned_exp_chn.groupby(["time"])
raw_hh_exp_per_capita_groups = raw_hh_exp_per_capita.groupby(["time", "country"])
years = list(set(cleaned_exp_chn["time"]))
cleaned_exp_proxy_countries = pd.DataFrame()
for year in years:
    logging.info("Find the household expenditure per capita of China")
    raw_hh_exp_chn_year = raw_hh_exp_per_capita_groups.get_group((str(year), "China"))
    raw_hh_exp_chn_year = raw_hh_exp_chn_year["value"].values[0]

    logging.info("Extract expenditure data of China in the year")
    cleaned_exp_chn_year = cleaned_exp_chn_groups.get_group((year,))
    for pos, country_ in enumerate(proxy_countries_worldbank):
        if country_ != "China":
            raw_hh_exp_country_year = raw_hh_exp_per_capita_groups.get_group(
                (str(year), country_)
            )
            raw_hh_exp_country_year = raw_hh_exp_country_year["value"].values[0]
            if not np.isnan(raw_hh_exp_country_year):
                logging.info("Calculate the scaling factors")
                scaling_factor = raw_hh_exp_country_year / raw_hh_exp_chn_year

                logging.info("Scaling the proxy expenditure data")
                cleaned_exp_proxy_country_year = cleaned_exp_chn_year.copy()
                cleaned_exp_proxy_country_year["country"] = proxy_countries_unpd[
                    pos
                ]  # make sure the country name is from UNPD
                cleaned_exp_proxy_country_year["value"] = (
                    cleaned_exp_proxy_country_year["value"]
                ) * scaling_factor
                cleaned_exp_proxy_country_year.drop(columns=["edu_level"], inplace=True)

                cleaned_exp_proxy_countries = pd.concat(
                    [cleaned_exp_proxy_countries, cleaned_exp_proxy_country_year]
                )
                del (
                    pos,
                    country_,
                    cleaned_exp_proxy_country_year,
                    raw_hh_exp_country_year,
                )
    del year, raw_hh_exp_chn_year, cleaned_exp_chn_year

# save data
logging.info("Save proxied data")
if not (path_data_clean.parent / country).exists():
    (path_data_clean.parent / country).mkdir(parents=True, exist_ok=True)

file_name = f"{country}_household_data_all_years_in_2005_usd.csv"
cleaned_exp_proxy_countries.to_csv(
    path_data_clean.parent / country / file_name, index=False
)
