# -*- coding: utf-8 -*-
"""
Created: Thur 6 Feb 2025
Description: Scripts to make plots to show CFPS data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""
import datetime
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import pandas as pd
import matplotlib.patches as patches
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from cmcrameri import cm
import math


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
path_data_clean = (
    data_home / "clean_data" / current_module / current_version / data_source
)

# specify a survey year
year = "2020"

logging.info(f"Load cleaned survey data for the year {year}")
input_file_name = f"cfps_personal_consum_by_category_{year}.csv"
personal_consum = pd.read_csv(path_data_clean / input_file_name)

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
    "pce",  # total consumption
]

logging.info("Specific age cohort")
# age_cohorts = [f"{(x-1)*5}-{(x-1)*5+4}" for x in range(5, 21)] + ["100+"]
# age_cohorts = [
#     "0-9",
#     "10-19",
#     "20-29",
#     "30-39",
#     "40-49",
#     "50-59",
#     "60-69",
#     "70-79",
#     "80-89",
#     "90+",
# ]

age_cohorts = [
    "0-19",
    "20-34",
    "34-49",
    "50-64",
    "65+",
]
num_cohorts = len(age_cohorts)


logging.info("Make the ridge plot")
logging.info("Specify colormap and colors")
fill_colors = [
    cm.batlow.colors[index * int(256 / num_cohorts)] for index in range(0, num_cohorts)
]

logging.info("Specify font size and line width")
font_size = 9
line_width = 1.5

logging.info("Specify width of band")
bandwidth = 1

for dependent_vari_ in consum_columns:
    dependent_vari = f"{dependent_vari_}_per_capita"

    personal_consum_no_nan = pd.DataFrame()
    for age_cohort in age_cohorts:
        try:
            age_start = int(age_cohort.split("-")[0])
        except ValueError:
            age_start = int(age_cohort.split("+")[0])
        try:
            age_end = int(age_cohort.split("-")[-1])
        except ValueError:
            age_end = 150

        subset_personal_consum = pd.DataFrame()
        subset_personal_consum_ = np.array(
            personal_consum.loc[
                (personal_consum[f"age_in_{year}"] >= age_start)
                & (personal_consum[f"age_in_{year}"] < age_end + 1),
                dependent_vari,
            ]
        )
        subset_personal_consum_ = subset_personal_consum_[
            ~np.isnan(subset_personal_consum_)
        ]

        quantiles = np.percentile(subset_personal_consum_, [25, 75]).tolist()
        personal_consum_max = quantiles[1] + 1.5 * (quantiles[1] - quantiles[0])
        personal_consum_min = quantiles[0] + 1.5 * (quantiles[1] - quantiles[0])

        subset_personal_consum[dependent_vari] = subset_personal_consum_[
            (subset_personal_consum_ > personal_consum_min)
            & (subset_personal_consum_ < personal_consum_max)
        ]

        subset_personal_consum["age_cohort"] = age_cohort
        personal_consum_no_nan = pd.concat(
            [personal_consum_no_nan, subset_personal_consum], ignore_index=True
        ).reset_index(drop=True)
        del age_cohort, subset_personal_consum, subset_personal_consum_

    sns.set_theme(
        style="white", rc={"axes.facecolor": (0, 0, 0, 0), "figure.figsize": (8, 2)}
    )

    # Initialize the FacetGrid object
    ridgeplot = sns.FacetGrid(
        personal_consum_no_nan,
        row="age_cohort",
        hue="age_cohort",
        aspect=10,
        height=0.3,
        palette=fill_colors,
    )

    # Draw the densities in a few steps
    ridgeplot.map(
        sns.kdeplot,
        dependent_vari,
        bw_adjust=bandwidth,
        clip_on=False,
        fill=True,
        alpha=1,
        linewidth=line_width,
    )
    ridgeplot.map(
        sns.kdeplot,
        dependent_vari,
        clip_on=False,
        color="w",
        lw=line_width,
        bw_adjust=bandwidth,
    )

    # passing color=None to refline() uses the hue mapping
    ridgeplot.refline(
        y=0, linewidth=line_width, linestyle="-", color=None, clip_on=False
    )

    # Define and use a simple function to label the plot in axes coordinates
    def label(x, color, label):
        ax = plt.gca()
        ax.text(
            -0.01,
            0.2,
            label,
            color=color,
            ha="right",
            va="center",
            transform=ax.transAxes,
        )

    ridgeplot.map(label, dependent_vari)

    # Set the subplots to overlap
    ridgeplot.figure.subplots_adjust(hspace=-0.25)

    # Remove axes details that don't play well with overlap
    ridgeplot.set_titles("")
    ridgeplot.set(yticks=[], ylabel="")
    ridgeplot.despine(bottom=True, left=True)
    plt.savefig(f"figure_{dependent_vari}.png", dpi=300, bbox_inches="tight")
    # plt.show()


# logging.info("Make the boxplot")
# fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(9, 9))
# for pos in range(len(consum_columns)):
#     logging.info("Specify the dependent variable")
#     dependent_vari = f"{consum_columns[pos]}_per_capita"

#     #########################################################################
#     # box plot
#     #########################################################################
#     personal_consum_no_nan = []
#     for age_cohort in age_cohorts:
#         try:
#             age_start = int(age_cohort.split("-")[0])
#         except ValueError:
#             age_start = int(age_cohort.split("+")[0])
#         try:
#             age_end = int(age_cohort.split("-")[-1])
#         except ValueError:
#             age_end = 150

#         subset_personal_consum_ = np.array(
#             personal_consum.loc[
#                 (personal_consum[f"age_in_{year}"] >= age_start)
#                 & (personal_consum[f"age_in_{year}"] < age_end + 1),
#                 dependent_vari,
#             ]
#         )
#         subset_personal_consum_ = subset_personal_consum_[
#             ~np.isnan(subset_personal_consum_)
#         ]
#         personal_consum_no_nan.append(subset_personal_consum_)
#         del age_cohort, subset_personal_consum_

#     logging.info("Make the subplot")
#     box_ = axs[math.floor(pos / 3), pos % 3].boxplot(
#         personal_consum_no_nan,
#         showfliers=False,
#         patch_artist=True,
#     )

#     axs[math.floor(pos / 3), pos % 3].yaxis.grid(True)
#     axs[math.floor(pos / 3), pos % 3].set_xticks(
#         [y + 1 for y in range(len(personal_consum_no_nan))],
#         labels=age_cohorts,
#     )

#     # axs.set_ylim(0, 15000)
#     if pos in [6, 7, 8]:
#         axs[math.floor(pos / 3), pos % 3].set_xlabel("Age cohorts")
#     axs[math.floor(pos / 3), pos % 3].set_ylabel(dependent_vari)

# plt.savefig(f"figure_personal_age_consumption.png", dpi=300, bbox_inches="tight")
# plt.show()
