# -*- coding: utf-8 -*-
"""
Created: Tuesday 14 January 2025
Description: Scripts to make plots to show CFPS data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from pathlib import Path
import datetime
import logging


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
input_file_name = f"cfps_personal_consum_by_category_{year}.csv"
person_consum = pd.read_csv(path_data_clean / input_file_name)

logging.info("Specify categories of consumption")
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

logging.info("Specific age cohort")
age_cohorts = [f"{(x-1)*5}-{(x-1)*5+4}" for x in range(5, 21)] + ["100+"]

person_consum_no_nan = pd.DataFrame()
for age_cohort in age_cohorts:
    try:
        age_start = int(age_cohort.split("-")[0])
    except ValueError:
        age_start = int(age_cohort.split("+")[0])
    try:
        age_end = int(age_cohort.split("-")[-1])
    except ValueError:
        age_end = 150

    # subset the data for each age cohort
    subset_person_consum = person_consum[
        (person_consum[f"age_in_{year}"] >= age_start)
        & (person_consum[f"age_in_{year}"] < age_end + 1)
    ].reset_index(drop=True)
    subset_person_consum["age_cohort"] = age_cohort
    person_consum_no_nan = pd.concat(
        [person_consum_no_nan, subset_person_consum], ignore_index=True
    )
num_cohorts = len(np.unique(person_consum_no_nan["age_cohort"]))


#########################################################################
# regression plot
#########################################################################
# Function to fit and predict polynomial (Environmental Kuznets Curve)
def fit_polynomial(x, y, degree=2):
    poly = PolynomialFeatures(degree=degree)
    x_poly = poly.fit_transform(x.reshape(-1, 1))
    model = LinearRegression().fit(x_poly, y)
    x_range = np.linspace(x.min(), x.max(), 100).reshape(-1, 1)
    y_pred = model.predict(poly.transform(x_range))
    r2 = r2_score(y, model.predict(x_poly))
    coef = model.coef_
    intercept = model.intercept_
    return x_range.flatten(), y_pred, coef, intercept, r2


logging.info("Create subplots")
fig, axes = plt.subplots(8, 1, figsize=(12, 18), sharex=False)

# Consumption categories and titles
titles = consum_columns.copy()

for pos, ax in enumerate(axes):
    measure = f"{consum_columns[pos]}_per_capita"

    x_ = np.array(person_consum_no_nan[f"age_in_{year}"])
    y_ = np.array(person_consum_no_nan[measure])
    nan_mask = ~np.isnan(x_) & ~np.isnan(y_)

    # Scatter by age group
    sns.scatterplot(
        data=person_consum_no_nan,
        x=f"age_in_{year}",
        y=measure,
        hue="age_cohort",
        ax=ax,
        s=100,
        palette="viridis",
        edgecolor="black",
        # label="By Age",
    )

    # Fit and plot EKC for age
    x_range, y_pred, coef, intercept, r2 = fit_polynomial(x_[nan_mask], y_[nan_mask])
    ax.plot(
        x_range,
        y_pred,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"EKC (Age, $R^2$={r2:.2f})",
    )
    func_str = f"Function: {intercept:.2f} + {coef[1]:.2f}x + {coef[2]:.2f}x²"
    ax.text(
        0.02,
        0.95,
        func_str,
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="top",
        color="red",
    )

    # # Scatter by household size
    # sns.scatterplot(
    #     data=df,
    #     x="household_size",
    #     y=measure,
    #     ax=ax,
    #     s=100,
    #     color="blue",
    #     edgecolor="black",
    #     label="By Household Size",
    # )

    # # Fit and plot EKC for household size
    # x_range, y_pred, coef, intercept, r2 = fit_polynomial(
    #     df["household_size"].values, df[measure].values
    # )
    # ax.plot(
    #     x_range,
    #     y_pred,
    #     color="green",
    #     linestyle="--",
    #     linewidth=2,
    #     label=f"EKC (Size, $R^2$={r2:.2f})",
    # )
    # func_str = f"Function: {intercept:.2f} + {coef[1]:.2f}x + {coef[2]:.2f}x²"
    # ax.text(
    #     0.02,
    #     0.88,
    #     func_str,
    #     transform=ax.transAxes,
    #     fontsize=10,
    #     verticalalignment="top",
    #     color="green",
    # )

    # Titles and labels
    ax.set_title(titles[pos], fontsize=14)
    ax.set_ylabel("Consumption", fontsize=12)
    ax.grid(alpha=0.3)
    ax.legend()

    del pos

# Add common x-axis label
plt.xlabel(f"Age in {year}", fontsize=12)
plt.tight_layout()
plt.show()
