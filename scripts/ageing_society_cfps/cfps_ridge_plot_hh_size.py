# -*- coding: utf-8 -*-
"""
Created: Friday 10 January 2025
Description: Scripts to make plots to show CFPS data
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
import seaborn as sns

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
import matplotlib.patches as patches
from matplotlib.lines import Line2D
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from cmcrameri import cm

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
input_file_name = f"cfps_household_consum_with_average_age_size_{year}.csv"
fam_consum = pd.read_csv(path_data_clean / input_file_name)

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


logging.info("Specify the dependent variable")
dependent_vari = "household_size"


#########################################################################
# ridge plot
#########################################################################
fam_consum_no_nan = pd.DataFrame()
for age_cohort in age_cohorts:
    try:
        age_start = int(age_cohort.split("-")[0])
    except ValueError:
        age_start = int(age_cohort.split("+")[0])
    try:
        age_end = int(age_cohort.split("-")[-1])
    except ValueError:
        age_end = 150

    # subset the data for each word
    subset_fam_consum = fam_consum[
        (fam_consum["average_age"] >= age_start)
        & (fam_consum["average_age"] < age_end + 1)
    ].reset_index(drop=True)
    subset_fam_consum["age_cohort"] = age_cohort
    fam_consum_no_nan = pd.concat(
        [fam_consum_no_nan, subset_fam_consum], ignore_index=True
    )
num_cohorts = len(np.unique(fam_consum_no_nan["age_cohort"]))

logging.info("Make the plot")

logging.info("Specify colormap and colors")
darkgreen = "#9BC184"
midgreen = "#C2D6A4"
lightgreen = "#E7E5CB"
colors = [lightgreen, midgreen, darkgreen, midgreen, lightgreen]
fill_colors = [
    cm.batlow.colors[index * int(256 / num_cohorts)] for index in range(0, num_cohorts)
]

logging.info("Specify font size and line width")
font_size = 9
line_width = 1.5

logging.info("Specify width of band")
bandwidth = 1


fig, axs = plt.subplots(nrows=num_cohorts, ncols=1, figsize=(8, 8))
axs = axs.flatten()  # needed to access each individual axis

for pos, age_cohort in enumerate(np.unique(fam_consum_no_nan["age_cohort"])):
    # subset the data for each word
    subset_fam_consum = fam_consum_no_nan[
        fam_consum_no_nan["age_cohort"] == age_cohort
    ].reset_index(drop=True)

    sns.kdeplot(
        subset_fam_consum[dependent_vari],
        shade=True,
        bw_adjust=bandwidth,
        ax=axs[pos],
        color=fill_colors[pos],
        edgecolor=fill_colors[pos],
        linewidth=line_width,
    )

    # national mean reference line
    median_dep_vari_size_chn = fam_consum[dependent_vari].median()
    axs[pos].axvline(median_dep_vari_size_chn, color="#525252", linestyle="--")

    # display average number of bedrooms on left
    axs[pos].text(
        -1,
        0,
        f"'{age_cohort}'",
        ha="left",
        fontsize=font_size,
        color="black",
    )

    # compute quantiles
    quantiles = np.percentile(
        subset_fam_consum[dependent_vari], [2.5, 10, 25, 75, 90, 97.5]
    )
    quantiles = quantiles.tolist()

    # fill space between each pair of quantiles
    for j in range(len(quantiles) - 1):
        axs[pos].fill_between(
            [quantiles[j], quantiles[j + 1]],  # lower bound  # upper bound
            0,  # max y=0
            0.4,  # max y=0.0002
            color="white",
            alpha=0.7,
            edgecolor=colors[j],
            linewidth=line_width,
        )

    # mean value as a reference
    median_dep_vari_size = subset_fam_consum[dependent_vari].median()
    axs[pos].scatter([median_dep_vari_size], [0.2], color="black", s=10)

    # set title and labels
    axs[pos].set_xlim(0, 12)
    axs[pos].set_ylim(0, 2)
    axs[pos].set_ylabel("")

    # x axis scale for last ax
    if pos == num_cohorts - 1:
        values_ = [0, 2, 4, 6, 8, 10, 12]
        for value_ in values_:
            axs[pos].text(value_, -0.6, f"{value_}", ha="center", fontsize=font_size)

    # remove axis
    axs[pos].set_axis_off()
# ---------------------------------------------------------------------------------
# reference line label
text = f"Median Line, {round(median_dep_vari_size_chn,1)} persons per household"
fig.text(0.29, 0.88, text, ha="left", fontsize=font_size)

# number of bedrooms label
text = "Age Cohort"
fig.text(
    0.04,
    0.88,
    text,
    ha="left",
    fontsize=font_size,
    color="black",
)

# x axis label
text = "Household size (persons per household)"
fig.text(0.5, 0.07, text, ha="center", fontsize=font_size)

subax = inset_axes(
    parent_axes=axs[num_cohorts - 2],  # the number_cohorts-2 row
    width="60%",
    height="250%",
    loc=4,
)
subax.set_xticks([])
subax.set_yticks([])
legend_subset = fam_consum_no_nan[
    fam_consum_no_nan["age_cohort"] == "35-39"
].reset_index(drop=True)
sns.kdeplot(
    legend_subset[dependent_vari],
    shade=True,
    ax=subax,
    bw_adjust=2,
    color="grey",
    edgecolor="lightgrey",
)
quantiles = np.percentile(legend_subset[dependent_vari], [2.5, 10, 25, 75, 90, 97.5])
quantiles = quantiles.tolist()
for j in range(len(quantiles) - 1):
    subax.fill_between(
        [quantiles[j], quantiles[j + 1]],  # lower bound  # upper bound
        0,  # max y=0
        0.1,  # max y=0.00004
        color="white",
        alpha=0.7,
        edgecolor=colors[j],
        linewidth=line_width,
    )
subax.set_xlim(-0.9, 12)
subax.set_ylim(-0.2, 0.4)
legend_subset_mean = legend_subset[dependent_vari].median()
subax.scatter([legend_subset_mean], [0.05], color="black", s=10)
subax.text(0, 0.32, "Legend", ha="left", fontsize=font_size, weight="bold")
subax.text(
    7,
    0.15,
    "Distribution\nof hosuehold size",
    ha="left",
    fontsize=font_size,
)


logging.info("Add percentages and arrows in the legend")


def add_arrow(head_pos, tail_pos, ax):
    style = "Simple, tail_width=0.01, head_width=1.5, head_length=2.5"
    kw = dict(arrowstyle=style, color="k", linewidth=line_width)
    arrow = patches.FancyArrowPatch(
        tail_pos, head_pos, connectionstyle="arc3,rad=.5", **kw
    )
    ax.add_patch(arrow)


subax.text(
    legend_subset_mean + 1,
    0.16,
    "Median",
    ha="center",
    fontsize=font_size,
)
add_arrow((legend_subset_mean, 0.06), (legend_subset_mean + 0.3, 0.15), subax)  # median


subax.text(
    8.1,
    -0.16,
    "95% of sizes",
    ha="center",
    fontsize=font_size,
)
add_arrow((6.5, 0), (7, -0.11), subax)  # 95%

subax.text(
    5,
    -0.16,
    "80% of sizes",
    ha="center",
    fontsize=font_size,
)
add_arrow((4.9, 0), (5, -0.11), subax)  # 80%

subax.text(
    1,
    -0.17,
    "50% of sizes \n within this range",
    ha="center",
    fontsize=font_size,
)
add_arrow((legend_subset_mean - 0.4, 0), (legend_subset_mean - 1, -0.11), subax)  # 50%
# ---------------------------------------------------------------------------------

plt.savefig(f"figure_{dependent_vari}.png", dpi=300, bbox_inches="tight")
plt.show()
