# -*- coding: utf-8 -*-
"""
Created: Wednesday 08 January 2025
Description: Scripts to analyze corelationships between consumption and household education and age from CFPS data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import json
import logging
from pathlib import Path
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
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

logging.info("Configure paths")
path_data_clean = data_home / "clean_data" / current_module / current_version

# specify a survey year
year = "2020"

logging.info(f"Load cleaned survey data for the year {year}")
cleaned_fam_consum = pd.read_csv(path_data_clean / f"cfps_household_data_{year}.csv")

consum_columns = [
    "daily",  # Household equipment and daily necessities expenditure
    "dress",  # Clothing and shoes expenditure
    "eec",  # Education and entertainment expenditure
    "food",  # Food expenditure
    "house",  # Residential expenditure
    "med",  # Health care expenditure
    "trco",  # Transportation and communication expenditure
    "other",  # Other expenditure
]


# calculate the average age of the household
def weighted_average_age(household_row: pd.Series):
    """
    To calculate weighted average age of each household
        - adult older than 20 (including), weight 1
        - person age between 10 (including) and 19 (including), weight 0.5
        - person younger than 10, weight 0.3

    Input parameter:
    household_row: pd.Series
        Each row from the family consumption DataFrame

    Return:
        average_age

    """
    age_columns = [
        column_name for column_name in household_row.keys() if "age_in_" in column_name
    ]

    household_ages = household_row[age_columns]
    edu_weights = np.select(
        [
            household_ages >= 20,
            (household_ages >= 10) & (household_ages <= 19),
            household_ages < 10,
        ],
        [1, 0.5, 0.3],
        default=0,  # Default weight for NaN
    )
    household_ages_valid = household_ages[~household_ages.isna()]  # Exclude NaN values
    edu_weights_valid = edu_weights[~household_ages.isna()]  # Corresponding weights
    if len(household_ages_valid) == 0:  # Handle households with no valid ages
        return np.nan
    return np.sum(household_ages_valid * edu_weights_valid) / np.sum(edu_weights_valid)


# calculate household size
def weighted_household_size(household_row: pd.Series):
    """
    To calculate household size each household, using Dependency Ratio. Weights are
        - adult older than 65 (including), weight 0.7
        - person age between 20 (including) and 64 (including), weight 1
        - person age between 10 (including) and 19 (including), weight 0.5
        - person younger than 9 (including), weight 0.3

    Input parameter:
    household_row: pd.Series
        Each row from the family consumption DataFrame

    Return:
        household_size
    """
    age_columns = [
        column_name for column_name in household_row.keys() if "age_in_" in column_name
    ]

    household_ages = household_row[age_columns]
    household_size_weights = np.select(
        [
            household_ages >= 65,
            (household_ages >= 20) & (household_ages < 65),
            (household_ages >= 10) & (household_ages < 20),
            household_ages < 10,
        ],
        [0.7, 1, 0.5, 0.3],
        default=0,  # Default weight for NaN
    )
    household_ages_valid = household_ages[~household_ages.isna()]  # Exclude NaN values
    household_size_weights_valid = household_size_weights[
        ~household_ages.isna()
    ]  # Corresponding weights
    if len(household_ages_valid) == 0:  # Handle households with no valid ages
        return np.nan
    return np.sum(household_size_weights_valid)


# Function to calculate the maximal and average educational level of the household
def cal_educational_level(household_row: pd.Series):
    """
    To calculate maxinmal and weighted average educational level of each household
        - adult older than 65 (including), educational level weight 0.7
        - person age between 20 (including) and 64 (including), educational level weight 1
        - person age between 10 (including) and 19 (including), educational level weight 0.5
        - person younger than 9 (including), educational level weight 0.3

    Input parameter:
    household_data: pd.DataFrame
        The family consumption DataFrame

    Return:
        pd.DataFrame() with two new columns representing maximal and weighted average educational level

    """
    # logging.info("Specify column names with educational levels")
    # person_edu_columns = [
    #     column_name
    #     for column_name in household_data.columns
    #     if "edu_pid_" in column_name
    # ]
    # logging.info("Specify column names of age")
    # age_columns = [
    #     column_name
    #     for column_name in household_data.columns
    #     if "age_in_" in column_name
    # ]

    # # Define a function to calculate weight based on age
    # def calculate_weight(age):
    #     if pd.isna(age):
    #         return np.nan
    #     elif age >= 65:
    #         return 0.7
    #     elif age >= 20:
    #         return 1
    #     elif age >= 10:
    #         return 0.5
    #     elif age < 10:
    #         return 0.3

    # logging.info("Apply weights according to person's age")
    # for age_column in age_columns:
    #     household_data[f"weight_{age_column}"] = household_data[age_column].apply(
    #         calculate_weight
    #     )

    # logging.info("Calculate maximal and weighted average educational level")
    # for pos, (person_column, age_column) in enumerate(
    #     zip(person_columns, age_columns), start=1
    # ):
    #     weight_column = f"weight_{age_column}"
    #     total_weights = household_data[
    #         [f"weight_{age_col}" for age_col in age_columns]
    #     ].sum(axis=1)
    #     for category in consum_columns:
    #         household_data[f"{person_column}_{category}"] = (
    #             household_data[category] * household_data[weight_column] / total_weights
    #         )

    # logging.info("Reshape the household dataset")
    # per_capita_consum_data = []
    # for pos, (person_column, age_column) in enumerate(
    #     zip(person_columns, age_columns), start=1
    # ):
    #     temp_household_data = household_data[
    #         [f"fid{year[2:]}", f"provcd{year[2:]}", person_column, age_column]
    #         + [f"{person_column}_{consum_column}" for consum_column in consum_columns]
    #     ].copy()

    #     temp_household_data.columns = [
    #         f"fid{year[2:]}",
    #         f"provcd{year[2:]}",
    #         "pid",
    #         f"age_in_{year}",
    #     ] + [f"{consum_column}_per_capita" for consum_column in consum_columns]
    #     per_capita_consum_data.append(temp_household_data)

    # reshaped_per_capita_consum_data = (
    #     pd.concat(per_capita_consum_data).dropna(subset=["pid"]).set_index("pid")
    # )

    # return reshaped_per_capita_consum_data
    age_columns = [
        column_name for column_name in household_row.keys() if "age_in_" in column_name
    ]

    household_edu_levels = household_row[person_edu_columns]
    household_ages = household_row[age_columns]
    edu_weights = np.select(
        [
            household_ages >= 65,
            (household_ages >= 20) & (household_ages < 65),
            (household_ages >= 10) & (household_ages < 20),
            household_ages < 10,
        ],
        [0.7, 1, 0.5, 0.3],
        default=0,  # Default weight for NaN
    )
    household_edu_valid = household_edu_levels[
        ~household_edu_levels.isna()
    ]  # Exclude NaN values
    edu_weights_valid = edu_weights[
        ~household_edu_levels.isna()
    ]  # Corresponding weights
    if len(household_edu_valid) == 0:  # Handle households with no valid ages
        return np.nan
    return np.sum(household_edu_valid * edu_weights_valid) / np.sum(edu_weights_valid)


# Function to calculate per-capita consumption
def weighted_per_capita_consum(household_data: pd.DataFrame):
    """
    To calculate per-capita consumption of each person, weights are:
        - adult older than 65 (including), weight 0.7
        - person age between 20 (including) and 64 (including), weight 1
        - person age between 10 (including) and 19 (including), weight 0.7
        - person younger than 9 (including), weight 0.5

    Input parameter:
    household_data: pd.DataFrame
        The family consumption DataFrame

    Return:
        household_size
    """

    logging.info("Specify column names with personal id (pid)")
    person_columns = [
        column_name
        for column_name in household_data.columns
        if "pid_pid_" in column_name
    ]
    logging.info("Specify column names of age")
    age_columns = [
        column_name for column_name in household_data.columns if "age_in" in column_name
    ]

    # Define a function to calculate weight based on age
    def calculate_weight(age):
        if pd.isna(age):
            return np.nan
        elif age >= 65:
            return 0.7
        elif age >= 20:
            return 1
        elif age >= 10:
            return 0.5
        elif age < 10:
            return 0.3

    logging.info("Apply weights according to person's age")
    for age_column in age_columns:
        household_data[f"weight_{age_column}"] = household_data[age_column].apply(
            calculate_weight
        )

    logging.info("Calculate per-capita consumption for each person and category")
    for pos, (person_column, age_column) in enumerate(
        zip(person_columns, age_columns), start=1
    ):
        weight_column = f"weight_{age_column}"
        total_weights = household_data[
            [f"weight_{age_col}" for age_col in age_columns]
        ].sum(axis=1)
        for category in consum_columns:
            household_data[f"{person_column}_{category}"] = (
                household_data[category] * household_data[weight_column] / total_weights
            )

    logging.info("Reshape the household dataset")
    per_capita_consum_data = []
    for pos, (person_column, age_column) in enumerate(
        zip(person_columns, age_columns), start=1
    ):
        temp_household_data = household_data[
            [f"fid{year[2:]}", f"provcd{year[2:]}", person_column, age_column]
            + [f"{person_column}_{consum_column}" for consum_column in consum_columns]
        ].copy()

        temp_household_data.columns = [
            f"fid{year[2:]}",
            f"provcd{year[2:]}",
            "pid",
            f"age_in_{year}",
        ] + [f"{consum_column}_per_capita" for consum_column in consum_columns]
        per_capita_consum_data.append(temp_household_data)

    reshaped_per_capita_consum_data = (
        pd.concat(per_capita_consum_data).dropna(subset=["pid"]).set_index("pid")
    )

    return reshaped_per_capita_consum_data


logging.info("Calculate weighed average age")
cleaned_fam_consum["average_age"] = cleaned_fam_consum.apply(
    weighted_average_age, axis=1
)

logging.info("Calculate weighed household size")
cleaned_fam_consum["household_size"] = cleaned_fam_consum.apply(
    weighted_household_size, axis=1
)

logging.info("Calculate maximal and weighed educational level of the household")
person_edu_columns = [
    column_name
    for column_name in cleaned_fam_consum.columns
    if "edu_pid_" in column_name
]
cleaned_fam_consum["average_edu"] = cleaned_fam_consum.apply(
    cal_educational_level, axis=1
)

cleaned_fam_consum["max_edu"] = cleaned_fam_consum[person_edu_columns].max(axis=1)
cleaned_fam_consum.to_csv("test_cleaned_fam_consum.csv", index=False)


logging.info("Calculate weighted consumption by person")
cleaned_person_consum = weighted_per_capita_consum(household_data=cleaned_fam_consum)


logging.info("Save data")
file_name = f"cfps_household_consum_with_average_age_size_{year}.csv"
cleaned_fam_consum.to_csv(path_data_clean / file_name, index=False)
del file_name

file_name = f"cfps_personal_consum_by_category_{year}.csv"
cleaned_person_consum.to_csv(path_data_clean / file_name, index=False)
del file_name

# # Exploratory Data Analysis (EDA)
# plt.scatter(cleaned_fam_consum["average_edu"], cleaned_fam_consum["eec"])
# plt.xlabel("Average Educational Level")
# plt.ylabel("Household Consumption")
# plt.title("Relationship between Average Age and Consumption")
# plt.show()


# # Statistical Analysis
# # correlation = cleaned_fam_consum[["average_age", "pce"]].corr()
# # print(correlation)

# logging.info("Omit rows with no average age data")
# cleaned_fam_consum = cleaned_fam_consum.dropna(subset=["average_age"])

# logging.info("Fill missing total consumption data")
# cleaned_fam_consum["pce"] = cleaned_fam_consum["pce"].fillna(
#     cleaned_fam_consum[consum_columns].sum(axis=1, skipna=True)
# )
# logging.info("Omit rows with no total consumption data")
# cleaned_fam_consum = cleaned_fam_consum.dropna(subset=["pce"])

# x_average_age = cleaned_fam_consum["average_age"].values.reshape(-1, 1)
# y_total_consum = cleaned_fam_consum["pce"]

# # # Linear regression
# # model = LinearRegression()
# # model.fit(x_average_age, y_total_consum)

# # print(f"Intercept: {model.intercept_}")
# # print(f"Coefficient: {model.coef_[0]}")

# # Non-linear regression
# poly_model = make_pipeline(PolynomialFeatures(degree=2), LinearRegression())
# poly_model.fit(x_average_age, y_total_consum)

# # Accessing the PolynomialFeatures step
# poly_features = poly_model.named_steps["polynomialfeatures"]
# print("Polynomial Features Attributes:")
# print(f"Degree: {poly_features.degree}")
# print(f"Include Bias: {poly_features.include_bias}")

# # Accessing the LinearRegression step
# linear_model = poly_model.named_steps["linearregression"]
# print("\nLinear Regression Attributes:")
# print(f"Coefficients: {linear_model.coef_}")
# print(f"Intercept: {linear_model.intercept_}")


# # Extract the fitting function
# def fitting_function(x):
#     # Polynomial expansion: [1, x, x^2]
#     expanded_x = poly_features.transform(np.array([[x]]))
#     return np.dot(expanded_x, linear_model.coef_) + linear_model.intercept_


# print("\nFitting Function:")
# coeffs = linear_model.coef_
# print(f"f(x) = {coeffs[0]} + {coeffs[1]}*x + {coeffs[2]}*x^2")
