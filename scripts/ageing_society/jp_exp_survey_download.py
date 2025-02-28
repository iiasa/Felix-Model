# -*- coding: utf-8 -*-
"""
Created: Monday 17 Feb 2025
Description: Scripts to download Japan (JP) expenditure survey data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.sc.at
"""

import datetime
import logging
from pathlib import Path

from dotenv import load_dotenv
import os
import requests
from bs4 import BeautifulSoup

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
data_source = "jp_exp_survey"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_raw.exists():
    path_data_raw.mkdir(parents=True, exist_ok=True)
if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Find available survey data in the webpage")
base_url = "https://www.stat.go.jp"
data_info_req = requests.get(f"{base_url}/english/data/sousetai/1.html")
soup = BeautifulSoup(data_info_req.content, "html.parser")

data_info = soup.find_all("a")
for data_info_ in data_info:
    try:
        data_text = data_info_.find("strong").text
    except AttributeError:
        continue

    if "yearly average" in data_text.lower():
        logging.info("Find the survey data page")
        logging.info("Extract the year infomation")
        year = data_text[:4]

        fact_page_url = data_info_["href"]

        fact_page_req = requests.get(f"{base_url}{fact_page_url}")
        soup_fact = BeautifulSoup(fact_page_req.content, "html.parser")

        facts_info = soup_fact.find_all("a")
        for fact_info in facts_info:
            try:
                fact_text = fact_info.text.lower()
            except AttributeError:
                continue

            if (
                ("receipt" in fact_text)
                and ("by number of" in fact_text)
                and ("by age group" in fact_text)
            ):
                fact_url = fact_info["href"]

                logging.info(f"Download the fact data from {fact_url}")
                if int(year) < 2021:
                    fact = requests.get(f"{base_url}/english/data/sousetai/{fact_url}")
                else:
                    fact = requests.get(f"{base_url}{fact_url}")

                # save data
                logging.info("Save raw data")
                file_name = f"jp_exp_survey_data_{year}.{fact_url.split('.')[-1]}"
                with open(path_data_raw / file_name, mode="wb") as fact_file:
                    fact_file.write(fact.content)
                del year, fact_url
