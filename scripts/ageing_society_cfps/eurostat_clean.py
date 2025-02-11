"""
Created: Tuesday 11 Feb 2025
Description: Scripts to match personal age and consumption data from eurostat
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
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
data_source = "eurostat"

logging.info("Configure paths")
path_data_raw = data_home / "raw_data" / current_module / current_version / data_source
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

if not path_data_clean.exists():
    path_data_clean.mkdir(parents=True, exist_ok=True)

logging.info("Specify file name of input data")
file_name_exp = "hbs_exp_t135__custom_15321137_spreadsheet.xlsx"
file_name_exp_str = "hbs_str_t225__custom_15321283_spreadsheet.xlsx"

logging.info("Read the expenditure data")
raw_exp = pd.read_excel(
    path_data_raw / file_name_exp,
    sheet_name="Sheet 1",
    skiprows=8,
    header=[0],
    dtype=str,
    skipfooter=4,
)
raw_exp = raw_exp.drop(index=0)
print(raw_exp)
exit()
raw_exp = raw_exp.loc[
    raw_exp["GEO (Labels)"].str.contains("Total")
    | raw_exp["GEO (Labels)"].str.contains("years"),
    :,
]

logging.info("Adjust column name")
raw_exp.columns = [column_.replace("*", "") for column_ in raw_exp.columns]
exit()

logging.info("Configure data years")
years = ["2014", "2016", "2018", "2020"]


def clean_data(file: Path):
    """Clean one raw data table for Norway.

    Parameters
    ----------
    df : DataFrame
        SUT data in wide form.

    Returns
    -------
    DataFrame
        Clean SUT in long form.
    """
    # Read general table info from first cells
    head = pd.read_excel(file, header=None, nrows=14, dtype=str)
    metadata = pd.concat([head.iloc[1:, 1], head.iloc[1:, 3]])
    metadata.index = pd.concat([head.iloc[1:, 0], head.iloc[1:, 2]])

    info = get_sut_info(head.loc[0, 0])
    for index, name in [
        ("TIME:", "time"),
        ("PRICE:", "price_type"),
        ("UNIT:", "unit"),
        ("UNIT_MULT:", "money_unit"),
    ]:
        value = metadata[index]
        if pd.notna(value) and "Col" not in value and "Row" not in value:
            if name == "time":
                info[name] = int(value)
            else:
                info[name] = value

    if "table_type" not in info:
        logger.info(f"Skipping '{head.loc[0,0]}', it contains no SUT.")
        return [None]
    else:
        logger.info(f"Cleaning table '{head.loc[0,0]}'.")

    # Read the sheet into a DataFrame
    df = pd.read_excel(file, skiprows=23, header=[0, 1, 3, 4], dtype=str)

    # Find the first col with data
    # start_col = int(metadata["CPA_FROM:"][-2:]) + 1  #FAILS because the files are not filled correctly
    start_col = (df.columns.get_level_values(1) == "S1").argmax()

    # Clean index columns
    codes, labels = df.columns[start_col - 2 : start_col]
    df = df[df[codes].notna() & df[labels].notna()]
    df.loc[df.iloc[:, 0].notna(), codes] = df.iloc[:, 0]
    df[labels] = df[labels].str.replace(r"\d+\)", "", regex=True)
    df[labels] = fix_whitespace(df[labels])
    df = df.iloc[:, 1:]

    # Clean column names - codes in headers[0], names in headers[2]
    headers = df.columns.to_frame(index=False)
    # to general unique codes for each activity, inlcuing final demand, and variations
    headers.loc[
        (headers[0] == "P1")
        | (headers[0] == "P2")
        | (headers[0] == "TP2") & (~headers[3].str.contains("Unnamed")),
        0,
    ] = headers[3]
    headers.loc[
        (headers[0] == "P3") | (headers[0] == "P6") | (headers[0] == "P7"), 0
    ] = headers[3].str.cat(headers[1], " | ")
    headers.loc[(headers[1] == "S") & (headers[3] == "Z"), 0] = headers[3].str.cat(
        headers[0], " | "
    )
    headers.loc[(headers[1] == "S1") & (headers[3] == "Z"), 0] = headers[3].str.cat(
        headers[0], " | "
    )

    # to clean the labels
    headers[2] = headers[2].str.replace(r"\d+\)", "", regex=True)
    headers[2] = fix_whitespace(headers[2])
    df.columns = pd.MultiIndex.from_frame(headers[[0, 2]])

    # fix the labels starting with " - "
    df.iloc[:, 1] = fix_whitespace(df.iloc[:, 1])

    # Melt DataFrame to long form
    long_df = df.melt(id_vars=list(df.columns[: start_col - 1]))
    long_df["value"] = pd.to_numeric(long_df.value, errors="coerce")
    long_df = long_df[long_df.value.notna() & (long_df.value != 0)]

    # Handle index columns
    columns = []
    for col in long_df.columns[: start_col - 3]:
        col_name = col[0].strip().lower()
        if "unit" in col_name:
            columns += [col_name.replace("unit multiplier", "money_unit")]
        else:
            del long_df[col]
    long_df.columns = columns + [
        "product_code",
        "product_name",
        "activity_code",
        "activity_name",
        "value",
    ]

    # Fill empty cells
    long_df.loc[
        long_df["product_name"]
        == "Total intermediate consumption/final use at purchasers' prices",
        "product_code",
    ] = "RADJ"

    long_df.loc[
        long_df["activity_code"].str.contains("TFUPR", na=False),
        "activity_name",
    ] = "Total final uses at purchasers' prices"

    long_df.loc[
        long_df["activity_code"].str.contains("TUPR", na=False),
        "activity_name",
    ] = "Total use at purchasers' prices"

    # Fill columns with info shared across data points
    for item in info:
        long_df[item] = info[item]

    if "unit" not in long_df.columns:
        logger.info(f"No units specified in {file.name}, assuming mln NOK.")
        long_df["unit"] = "NOK"
        long_df["money_unit"] = "million"

    long_df["unit"] = long_df["unit"].replace("NATCUR", "NOK")
    long_df["money_unit"] = long_df["money_unit"].replace(
        {"3": "thousand", "6": "million"}
    )

    return long_df
