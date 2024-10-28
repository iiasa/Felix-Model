# -*- coding: utf-8 -*-
"""
Created: Thur 15 Feburary 2024
Description: Scripts to download World Bank data via api
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
path_raw_data_folder = Path("data_regionalization_raw/version_nov2023/world_bank")
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
api_info["base_url"] = "https://api.worldbank.org/v2/"
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
    params = {
        "format": "json",
        "per_page": 50000,
    }
    dataset_request = requests.get(
        f"{api_info['base_url']}indicator",
        params=params,
    )
    dataset_info = []
    if dataset_request.json()[0]["total"] <= params["per_page"]:
        datasets = dataset_request.json()[1]
        for dataset in datasets:
            entry = {}
            for key in dataset.keys():
                if key == "source":
                    entry[f"{key}_id"] = dataset[key]["id"]
                    entry[f"{key}_value"] = dataset[key]["value"]
                elif key == "topics":
                    for pos, topic in enumerate(dataset[key]):
                        try:
                            entry[f"topic_{pos+1}_id"] = dataset[key][pos]["id"]
                            entry[f"topic_{pos+1}_value"] = dataset[key][pos]["value"]
                        except KeyError:
                            pass
                else:
                    entry[key] = dataset[key]
            dataset_info.append(entry)
            del entry
    else:
        logging.warning("More datasets are there.")
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

    """
    logging.info(f"Start to get data cube '{endpoint}'")
    base_url = api_info["base_url"]

    logging.info(f"Query the source ID for the data cube '{endpoint}'")
    dataset_info = requests.get(
        url=f"{base_url}indicator/{endpoint}",
        params={
            "format": "json",
        },
    ).json()
    try:
        source_id = dataset_info[1][0]["source"]["id"]
    except KeyError:
        raise KeyError("No source id, please double check")
    endpoint_text = dataset_info[1][0]["name"]
    logging.info(f"Query the data cube '{endpoint}'")
    # Get facts
    logging.info(f"Configuring facts of {endpoint} by location")
    # query is a list of 'values' from the list of dictionaries dimensions['variables']
    # Note: create list rather than append for performance
    query = [
        f"sources/{source_id}",
        "country/all",
        f"series/{endpoint}",
        "time/all",
    ]

    params = {
        "per_page": 10000000,
    }

    logging.info(f"Getting facts of {endpoint} with query {query}")
    facts_request = requests.get(
        url=f"{base_url}{'/'.join(query)}/data",
        params=params,
    )

    logging.info(f"Checking whether queried the full dataset")
    logging.info(f"Getting dimensions of {endpoint} from facts")
    soup = BeautifulSoup(facts_request.content, "xml")
    data_point = soup.find("data")["total"]
    if int(data_point) > params["per_page"]:
        raise Warning(
            f"More data available, total number of data points is {data_point}"
        )

    # Get dimensions
    dimensions = {
        "text": endpoint_text,
        "last_updated": soup.find("data")["lastupdated"],
        "source_id": soup.find("source")["id"],
        "source_name": soup.find("source")["name"],
    }
    facts = []
    fact_points = soup.find_all("source")[0].find_all("data")
    for fact in fact_points:
        variables = fact.find_all("variable")
        if "variable" not in dimensions.keys():
            dimensions["variable"] = len(variables) * [None]
            for pos in range(len(variables)):
                dimensions["variable"][pos] = {
                    "concept": None,
                    "values": [],
                    "value_texts": [],
                }

        entry = {}
        for pos, variable in enumerate(variables):
            var_concept = variable["concept"].lower()
            var_id = variable["id"]
            var_text = variable.text

            dimensions["variable"][pos]["concept"] = var_concept
            if var_id not in dimensions["variable"][pos]["values"]:
                dimensions["variable"][pos]["values"].append(var_id)
                dimensions["variable"][pos]["value_texts"].append(var_text)
            entry[var_concept] = var_text
            del pos, variable

        value = fact.find("value").text
        if value:
            value = float(value)
        entry["value"] = value
        facts.append(entry)
        del entry, variables, value

    # dimensions has onw field: variables, we add the next two
    # logger.info(f"The title of dimensions is: {dimensions['title']}")
    dimensions["download_date"] = timestamp
    dimensions["data_cube"] = endpoint
    # endpoints have .px suffix, using stem as root to output file names

    return dimensions, facts


# Get dataset info
# logging.info("Get dataset info from the API")
# dataset_info = get_dataset_info(api_info=api_info)
# dataset_info.to_csv(
#     path_raw_data_folder / "dataset_info.csv",
#     index=False,
# )
# logging.info("Got all dataset info from the API")


# Get datacube for a specified dataset
logging.info("Specify id for the dataset and output path")
endpoint = "NV.IND.TOTL.KD"
module = "gdp"
version = "nov2023"
path_data_output = path_raw_data_folder.parent.parent / f"version_{version}" / module

logging.info(f"Get datacube {endpoint} via the API")
dimensions, facts = get_data_cube(
    api_info=api_info,
    endpoint=endpoint,
)

logging.info(f"Writing dimensions of {endpoint}")
dim_name = f"{endpoint.replace('.','_')}_dim.yaml"


with open(Path(path_data_output).joinpath(dim_name), "w") as f:
    yaml.safe_dump(dimensions, f)
logging.info(f"Finished dimensions of {endpoint}")


logging.info(f"Writing facts of {endpoint}")
#  Using zip because xml is very large
fact_name = f"{endpoint.replace('.','_')}_fact.json"

with open(path_data_output.joinpath(fact_name), "w") as zf:
    zf.write(json.dumps(facts))
