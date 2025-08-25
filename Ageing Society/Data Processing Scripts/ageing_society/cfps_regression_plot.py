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
from scipy.optimize import curve_fit


from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_AGEING_SOCIETY")


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
current_project = "ageing_society"

logging.info("Configure data source")
data_source = "cfps"

logging.info("Configure paths")
path_data_clean = (
    data_home / "clean_data" / current_project / current_version / data_source
)

####################################
# specify a survey year
year = "2020"
####################################

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
age_cohorts = [f"{i}-{i}" for i in range(100)] + ["100+"]
#     "0-14",
#     "15-24",
#     "25-44",
#     "45-64",
#     "65+",
# ]

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

logging.info("Get the age and educational levels")
age_variables = np.unique(person_consum_no_nan[f"age_in_{year}"])
edu_levels = np.unique(person_consum_no_nan[f"edu_in_{year}"])


####################################
# specify the dependent variable, edu or age
dep_variable = "age"
####################################
if dep_variable == "age":
    dep_variable_values = age_variables
    del age_cohorts
elif dep_variable == "edu":
    dep_variable_values = edu_levels
    del edu_levels


# regression plot
# Function to fit and predict polynomial (Environmental Kuznets Curve)
def fit_polynomial(x, y, degree=4):
    poly = PolynomialFeatures(degree=degree)
    x_poly = poly.fit_transform(x.reshape(-1, 1))
    model = LinearRegression().fit(x_poly, y)
    x_range = np.linspace(x.min(), x.max(), 100).reshape(-1, 1)
    y_pred = model.predict(poly.transform(x_range))
    r2 = r2_score(y, model.predict(x_poly))
    coef = model.coef_
    intercept = model.intercept_
    return x_range.flatten(), y_pred, coef, intercept, r2


def omit_outlier(data_series: np.array):
    """
    To omit outliers of a data series

    Input parameters:
    data_series: np.array
        A data series that needs to omit outliers

    Return:
        data_series_without_outliers
    """
    logging.info("Omit nan values")
    data_series_ = data_series[~np.isnan(data_series)]

    logging.info("Calculate the 5 and 95 quantiles")
    quantiles = np.percentile(data_series_, [50, 92.5]).tolist()

    data_series_without_outliers = data_series_[
        (data_series_ >= quantiles[0]) & (data_series_ <= quantiles[-1])
    ]

    if len(data_series_without_outliers) > 0:
        return data_series_without_outliers, quantiles[-1], quantiles[0]
    else:
        logging.warning("No valid values left")
        return data_series_, quantiles[-1], quantiles[0]


logging.info("Create subplots")
fig, axes = plt.subplots(2, 1, figsize=(12, 18), sharex=False)

# Consumption categories and titles
titles = consum_columns.copy()
for pos, ax in enumerate(axes):
    measure = f"{consum_columns[pos]}_per_capita"

    x_plot = []
    y_plot = []
    person_consum_no_nan_groups = person_consum_no_nan[
        [f"{dep_variable}_in_{year}", measure]
    ].groupby(f"{dep_variable}_in_{year}")

    for dep_vari_value in dep_variable_values:
        person_consum_ = person_consum_no_nan_groups.get_group(dep_vari_value)
        y_ = np.array(person_consum_[measure])
        y_, y_max, y_min = omit_outlier(data_series=y_)

        x_ = np.array(
            person_consum_.loc[
                (person_consum_[measure] > y_min) & (person_consum_[measure] <= y_max),
                f"{dep_variable}_in_{year}",
            ]
        )

        x_plot.append(x_.mean())
        y_plot.append(y_.mean())
        del x_, y_

    mask = ~np.isnan(np.array(x_plot)) & ~np.isnan(np.array(y_plot))
    x_plot = np.array(x_plot)[mask]
    y_plot = np.array(y_plot)[mask]

    # Scatter by age group
    sns.scatterplot(
        data=pd.DataFrame(
            {f"{dep_variable}_in_{year}": list(x_plot), measure: list(y_plot)}
        ),
        x=f"{dep_variable}_in_{year}",
        y=measure,
        # hue="age_cohort",
        ax=ax,
        s=100,
        palette="viridis",
        edgecolor="black",
        # label="By Age",
    )

    # # Fit based on the logistic function
    # def logistic_functions(
    #     x: float,
    #     x_ref: float,
    #     y_ref: float,
    #     L0: float,
    #     L: float,
    #     k: float,
    #     x0: float,
    # ):
    #     """
    #     Define different logistic equations

    #     Parameter
    #     ---------
    #     x: float
    #         The value of dependency variable

    #     x_ref: float
    #         The reference value of dependency variable, mostly using the value in 2000

    #     y_ref: float,
    #         The reference value of dependency variable, mostly using the value in 2000

    #     L0: float
    #         Parameter used in the logistic function

    #     L: float
    #         Parameter used in the logistic function

    #     k: float
    #         Parameter used in the logistic function

    #     x0: float
    #         Parameter used in the logistic function

    #     Returns
    #     -------
    #     The estimated value of the calbration variable
    #     """

    #     exp_term = np.exp(-k * (x / x_ref - x0))
    #     return (L0 + L / (1 + exp_term)) * y_ref

    # iteration_time = 50000
    # n_par, n_cov = curve_fit(
    #     logistic_functions,
    #     list(x_plot),
    #     list(y_plot),
    #     maxfev=iteration_time,
    #     bounds=(
    #         [
    #             min(list(x_plot)),  # for x_ref
    #             min(list(y_plot)),  # for y_ref
    #             0,  # for L0
    #             -np.inf,  # for L
    #             -np.inf,  # for k
    #             0,  # for x0
    #         ],
    #         [
    #             max(list(x_plot)),  # for x_ref
    #             max(list(y_plot)),  # for y_ref
    #             np.inf,  # for L0
    #             np.inf,  # for L
    #             np.inf,  # for k
    #             np.inf,  # for x0
    #         ],
    #     ),
    # )

    # y_est = logistic_functions(np.array(list(x_plot)), *n_par)

    # # exit()

    # Fit and plot EKC for age
    # x_range, y_pred, coef, intercept, r2 = fit_polynomial(x_plot, y_plot, degree=4)
    # ax.plot(
    #     x_range,
    #     y_pred,
    #     color="red",
    #     linestyle="--",
    #     linewidth=2,
    #     # label=f"EKC (Age, $R^2$={r2:.2f})",
    # )
    # func_str = f"Function: {intercept:.2f} + {coef[1]:.2f}x + {coef[2]:.2f}x²"
    # ax.text(
    #     0.02,
    #     0.95,
    #     func_str,
    #     transform=ax.transAxes,
    #     fontsize=10,
    #     verticalalignment="top",
    #     color="red",
    # )

    # Titles and labels
    ax.set_title(titles[pos], fontsize=14)
    ax.set_ylabel("Consumption", fontsize=12)
    ax.grid(alpha=0.3)
    ax.legend()

    del pos
    # print(
    #     r2_score(
    #         y_plot,
    #         y_est,
    #     )
    # )
    # Add common x-axis label
    plt.xlabel(f"{dep_variable} in {year}", fontsize=12)
    plt.tight_layout()
    plt.show()
    exit()
