# -*- coding: utf-8 -*-
"""
Created: Tue 13 Feburary 2024
Description: Scripts to download UNPD data via api
Scope: FeliX model regionalization
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""
import datetime
import json
import logging
import os
from io import BytesIO
from pathlib import Path
from time import time

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()
path_raw_data_folder = Path("data_regionalization_raw/version_nov2023/undata_unpd")
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
logging.info("Start query information from the UN data API")
logging.info("Set up basic information about the UN data API")
api_info = {}
api_info["base_url"] = "https://population.un.org/dataportalapi/api/v1/"
api_info["lang"] = "en"
for secret in ["API_USERNAME", "API_PASSWORD", "API_TOKEN", "API_KEY"]:
    api_info[secret] = os.getenv(secret)
    secret_str = "" if api_info[secret] else "not "
logging.info("api_info created")


def get_dataset_info(
    api_info: str,
) -> pd.DataFrame:
    """
    Collect IDs of all datasets.

    Parameters
    ----------
    api_info: dict
        A dictionary includes all the information to call the API,
        such as key, token, etc.

    """
    t0 = time()
    logging.info("Querying base url for list of datasets")
    dataset_request = requests.get(
        f"{api_info['base_url']}indicators",
    )

    dataset_info = dataset_request.json()["data"]
    logging.info(f"Queried in {(time() - t0):.2f}s")
    # dataset_id is list of dictionaries, changing into dataframe
    dataset_df = pd.DataFrame(dataset_info)
    logging.info("Transformed list of dictionaries to dataframe")
    return dataset_df


def get_data_cube(
    api_info: dict,
    endpoint: str,
):
    """Get data cube and save dimension and fact files

    Parameters
    ----------
    base_url: str
    endpoint: str

    Returns
    -------
    dimensions: dict
    fact: pd.DataFrame
    """
    logging.info(f"Start to get data cube '{endpoint}'")

    # Get dimensions
    logging.info(f"Getting dimensions of {endpoint}")
    base_url = api_info["base_url"]
    dimensions_request = requests.get(f"{base_url}indicators/{endpoint}?sort=id")
    dimensions = dimensions_request.json()[0]

    logging.info(f"Getting dimensions of locations")
    dimension_loc = requests.get(f"{base_url}locations?sort=id").json()
    pages = int(dimension_loc["pages"])

    values = []
    valueTexts = []
    for page in range(1, pages + 1):
        dimension_loc = requests.get(
            f"{base_url}locations?sort=id&pageNumber={page}"
        ).json()

        locations = dimension_loc["data"]
        for location in locations:
            values.append(location["id"])
            valueTexts.append(location["name"])
            del location

        del locations, dimension_loc
    dimensions["locations"] = {
        "value": values,
        "text": valueTexts,
    }
    # dimensions has onw field: variables, we add the next two
    # logger.info(f"The title of dimensions is: {dimensions['title']}")
    dimensions["download_date"] = timestamp
    dimensions["data_cube"] = endpoint
    # endpoints have .px suffix, using stem as root to output file names

    # Get facts
    facts = pd.DataFrame()
    for loc_id in dimensions["locations"]["value"]:
        logging.info(f"Configuring facts of {endpoint} by location")
        # query is a list of 'values' from the list of dictionaries dimensions['variables']
        # Note: create list rather than append for performance
        query = [
            f"indicators/{endpoint}",
            f"locations/{loc_id}",
            f"start/{dimensions['sourceStartYear']}",
            f"end/{dimensions['sourceEndYear']}",
        ]

        logging.info(f"Getting facts of {endpoint} in location id {loc_id}")
        fact_request = requests.get(
            url=f"{base_url}data/{'/'.join(query)}?format=csv",
        )
        if fact_request.status_code == 200:
            fact = pd.read_csv(
                BytesIO(fact_request.content),
                sep="|",
                low_memory=False,
                skiprows=1,
            )
            fact["SexId"] = fact["SexId"].astype("int")
            fact["VariantId"] = fact["VariantId"].astype("int")

            if endpoint in [58]:  # data only available for both sex
                facts = pd.concat(
                    [
                        facts,
                        fact.loc[
                            (fact["VariantId"] == 4) & (fact["SexId"] == 3)
                        ],  # variantId=4, median
                    ],
                    ignore_index=True,
                )
            else:
                facts = pd.concat(
                    [
                        facts,
                        fact.loc[
                            (fact["VariantId"] == 4) & (fact["SexId"] < 3)
                        ],  # variantId=4, median
                    ],
                    ignore_index=True,
                )
            del fact_request, fact, query

            text = dimensions["shortName"].lower()
            #  Using zip because xml is very large
            fact_name = f"{text}_fact.csv"

            facts.to_csv(
                Path(path_data_output).joinpath(fact_name),
                index=False,
            )
            del text, fact_name
        else:
            continue

    return dimensions, facts


# # Get dataset info
# logging.info("Get dataset info from the API")
# dataset_info = get_dataset_info(api_info=api_info)
# dataset_info.to_csv(
#     path_raw_data_folder / "dataset_info.csv",
#     index=False,
# )
# logging.info("Got all dataset info from the API")


# Get datacube for a specified dataset
logging.info("Specify id for the dataset and output path")
endpoint = 57
module = "birth_rate"
version = "nov2023"
path_data_output = path_raw_data_folder.parent.parent / f"version_{version}" / module

logging.info(f"Get datacube {endpoint} via the API")
dimensions, facts = get_data_cube(
    api_info=api_info,
    endpoint=endpoint,
)

logging.info(f"Writing dimensions of {endpoint}")
text = dimensions["shortName"].lower()
dim_name = f"{text}_dim.yaml"


with open(Path(path_data_output).joinpath(dim_name), "w") as f:
    yaml.safe_dump(dimensions, f)
logging.info(f"Finished dimensions of {endpoint}")


logging.info(f"Writing facts of {endpoint}")
#  Using zip because xml is very large
fact_name = f"{text}_fact.csv"

facts.to_csv(
    Path(path_data_output).joinpath(fact_name),
    index=False,
)
