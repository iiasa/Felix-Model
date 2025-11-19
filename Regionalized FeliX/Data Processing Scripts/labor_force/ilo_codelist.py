# -*- coding: utf-8 -*-
"""
Created: Mon 03 June 2024
Description: Scripts to query codelists for dimensions of ILO
Scope: FeliX model regionalization
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""
import datetime
import json
import logging
import os
from pathlib import Path
from time import time
from bs4 import BeautifulSoup

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()
path_raw_data_folder = Path("data_regionalization_raw/version_nov2023/labor_force")
if not path_raw_data_folder.exists():
    path_raw_data_folder.mkdir(
        parents=True,
    )

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# set logging
if not (path_raw_data_folder.joinpath("logs")).exists():
    (path_raw_data_folder.joinpath("logs")).mkdir(
        parents=True,
    )

logging.basicConfig(
    level=logging.DEBUG,
    filename=f"{path_raw_data_folder}/logs/{timestamp.strftime('%d-%m-%Y')}.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# add the handler to the root logging
logging.getLogger().addHandler(console)

# BASIC INFORMATION
logging.info("Start query information from the ILO API")
logging.info("Set up basic information about the ILO API")
api_info = {}
api_info["base_url"] = "https://sdmx.ilo.org/rest/"
api_info["lang"] = "en"
for secret in ["API_USERNAME", "API_PASSWORD", "API_TOKEN", "API_KEY"]:
    api_info[secret] = os.getenv(secret)
    secret_str = "" if api_info[secret] else "not "
logging.info("api_info created")


def get_codelist(
    dimension_ids: list,
    api_info: str,
) -> pd.DataFrame:
    """
    Collect codelists of specified dimension.

    Parameters
    ----------
    dimension_id: list
        A list of specified dimension ids, e.g., CL_AREA for reference ares, CL_SEX for sex
    api_info: dict
        A dictionary includes all the information to call the API,
        such as key, token, etc.

    """
    t0 = time()
    logging.info(f"Getting all codelists for each dimension")
    dimensions = {
        "variables": [],
    }
    for dimension_id in dimension_ids:
        dim_request = requests.get(f"{api_info['base_url']}codelist/ILO/{dimension_id}")
        soup = BeautifulSoup(dim_request.content, "xml")
        del dim_request

        dimension_text = soup.find("common:Name").text.split(": ")[-1].lower()
        parameter_values = soup.find_all("structure:Code")
        values = []
        valueTexts = []
        for value in parameter_values:
            values.append(value["id"])
            try:
                value_text = [
                    text_.text
                    for text_ in value.find_all("common:Name")
                    if text_.get("xml:lang") == "en"
                ][0]
            except IndexError:
                try:
                    value_text = [
                        text_.text
                        for text_ in value.find_all("common:Name")
                        if text_.get("xml:lang") == "fr"
                    ][0]
                except IndexError:
                    value_text = [
                        text_.text
                        for text_ in value.find_all("common:Name")
                        if text_.get("xml:lang") == "es"
                    ][0]
            valueTexts.append(value_text)

        dimensions["variables"].append(
            {
                "code": dimension_id,
                "text": dimension_text,
                "values": values,
                "valueTexts": valueTexts,
            }
        )
        del values, valueTexts
    # dimensions has onw field: variables, we add the next two
    # logger.info(f"The title of dimensions is: {dimensions['title']}")
    dimensions["download_date"] = timestamp

    logging.info(f"Finished dimensions in " f"{(time() - t0):.2f} s")

    return dimensions


# Get datacube for a specified dataset
logging.info("Specify id for the dataset and output path")
dimension_ids = [
    "CL_AREA",
    "CL_SEX",
    "CL_AGE_5YRBANDS",
]
module = "ilo"
version = "nov2023"
path_data_output = path_raw_data_folder.parent.parent / f"version_{version}" / module

logging.info(f"Get codelist of dimensions via the ILO API")
dimension_info = get_codelist(
    dimension_ids=dimension_ids,
    api_info=api_info,
)

logging.info(f"Writing codelists of dimensions")
dim_name = f"dimmension_codelists_ilo.yaml"

logging.info(f"Writing dimensions")
with open(Path(path_raw_data_folder).joinpath(dim_name), "w") as f:
    yaml.safe_dump(dimension_info, f)
