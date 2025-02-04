# -*- coding: utf-8 -*-
"""
Created: Tuesday 14 January 2025
Description: Scripts to make plots to show CFPS data
Scope: Ageing society project, module ageing_society
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

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

exit()
# Define age groups
bins = [20, 30, 40, 50, 60, 70]
labels = ["20-29", "30-39", "40-49", "50-59", "60-69"]
df["age_group"] = pd.cut(df["average_age"], bins=bins, labels=labels, right=False)


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


# Create subplots
fig, axes = plt.subplots(3, 1, figsize=(12, 18), sharex=False)

# Consumption categories and titles
consumption_measures = ["total_consum", "food_consum", "energy_consum"]
titles = ["Total Consumption", "Food Consumption", "Energy Consumption"]

for i, ax in enumerate(axes):
    measure = consumption_measures[i]

    # Scatter by age group
    sns.scatterplot(
        data=df,
        x="average_age",
        y=measure,
        hue="age_group",
        ax=ax,
        s=100,
        palette="viridis",
        edgecolor="black",
        # label="By Age",
    )

    # Fit and plot EKC for age
    x_range, y_pred, coef, intercept, r2 = fit_polynomial(
        df["average_age"].values, df[measure].values
    )
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

    # Scatter by household size
    sns.scatterplot(
        data=df,
        x="household_size",
        y=measure,
        ax=ax,
        s=100,
        color="blue",
        edgecolor="black",
        label="By Household Size",
    )

    # Fit and plot EKC for household size
    x_range, y_pred, coef, intercept, r2 = fit_polynomial(
        df["household_size"].values, df[measure].values
    )
    ax.plot(
        x_range,
        y_pred,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"EKC (Size, $R^2$={r2:.2f})",
    )
    func_str = f"Function: {intercept:.2f} + {coef[1]:.2f}x + {coef[2]:.2f}x²"
    ax.text(
        0.02,
        0.88,
        func_str,
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="top",
        color="green",
    )

    # Titles and labels
    ax.set_title(titles[i], fontsize=14)
    ax.set_ylabel("Consumption", fontsize=12)
    ax.grid(alpha=0.3)
    ax.legend()

# Add common x-axis label
plt.xlabel("Average Age / Household Size", fontsize=12)
plt.tight_layout()
plt.show()


exit()


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

# Configure paths
root_path = Path("./cfps_data")
path_data_clean = root_path / "cfps_clean"

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

# logging.info("Add backgrounf lines")


# def add_line(xpos, ypos, fig=fig):
#     line = Line2D(xpos, ypos, color="lightgrey", lw=0.5, transform=fig.transFigure)
#     fig.lines.append(line)


# add_line([4.95 / 12.9, 4.95 / 12.9], [0.11, 0.88])
# add_line([6.61 / 12.9, 6.61 / 12.9], [0.11, 0.88])
# add_line([8.28 / 12.9, 8.28 / 12.9], [0.11, 0.88])
# add_line([9.95 / 12.9, 9.95 / 12.9], [0.11, 0.88])
# add_line([11.58 / 12.9, 11.58 / 12.9], [0.11, 0.88])


for pos, age_cohort in enumerate(np.unique(fam_consum_no_nan["age_cohort"])):
    # subset the data for each word
    subset_fam_consum = fam_consum_no_nan[
        fam_consum_no_nan["age_cohort"] == age_cohort
    ].reset_index(drop=True)

    sns.kdeplot(
        subset_fam_consum["household_size"],
        shade=True,
        bw_adjust=bandwidth,
        ax=axs[pos],
        color=fill_colors[pos],
        edgecolor=fill_colors[pos],
        linewidth=line_width,
    )

    # national mean reference line
    median_hh_size_chn = fam_consum["household_size"].median()
    axs[pos].axvline(median_hh_size_chn, color="#525252", linestyle="--")

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
        subset_fam_consum["household_size"], [2.5, 10, 25, 75, 90, 97.5]
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
    median_hh_size = subset_fam_consum["household_size"].median()
    axs[pos].scatter([median_hh_size], [0.2], color="black", s=10)

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

# reference line label
text = f"Median Line, {round(median_hh_size_chn,1)} persons per household"
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
    legend_subset["household_size"],
    shade=True,
    ax=subax,
    bw_adjust=2,
    color="grey",
    edgecolor="lightgrey",
)
quantiles = np.percentile(legend_subset["household_size"], [2.5, 10, 25, 75, 90, 97.5])
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
legend_subset_mean = legend_subset["household_size"].median()
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


plt.savefig("figure_household_size.png", dpi=300, bbox_inches="tight")
plt.show()


# sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})

# # Create the data
# rs = np.random.RandomState(1979)
# x = rs.randn(500)
# g = np.tile(list("ABCDEFGHIJ"), 50)
# df = pd.DataFrame(dict(x=x, g=g))
# m = df.g.map(ord)
# df["x"] += m

# fam_consum_age_cohorts = pd.DataFrame()
# for pos, age_cohort in enumerate(age_cohorts):
#     try:
#         age_start = int(age_cohort.split("-")[0])
#     except ValueError:
#         age_start = int(age_cohort.split("+")[0])
#     try:
#         age_end = int(age_cohort.split("-")[-1])
#     except ValueError:
#         age_end = 150

#     # subset the data for each word
#     subset_fam_consum = fam_consum[
#         (fam_consum["average_age"] >= age_start)
#         & (fam_consum["average_age"] < age_end + 1)
#     ].reset_index(drop=True)
#     subset_fam_consum["age_cohort"] = age_cohort
#     fam_consum_age_cohorts = pd.concat(
#         [fam_consum_age_cohorts, subset_fam_consum],
#         ignore_index=True,
#     )
#     del age_cohort, pos

# num_cohorts = len(np.unique(fam_consum_age_cohorts["age_cohort"]))


# # Initialize the FacetGrid object
# pal = sns.cubehelix_palette(n_colors=num_cohorts, rot=-0.25, light=0.7)
# g = sns.FacetGrid(
#     fam_consum_age_cohorts[["household_size", "age_cohort"]],
#     row="age_cohort",
#     hue="age_cohort",
#     aspect=15,
#     height=0.5,
#     palette=pal,
# )

# # Draw the densities in a few steps
# g.map(
#     sns.kdeplot,
#     "household_size",
#     bw_adjust=0.5,
#     clip_on=False,
#     fill=True,
#     alpha=1,
#     linewidth=1.5,
# )
# g.map(sns.kdeplot, "household_size", clip_on=False, color="w", lw=2, bw_adjust=0.5)

# # passing color=None to refline() uses the hue mapping
# g.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)


# # Define and use a simple function to label the plot in axes coordinates
# def label(x, color, label):
#     ax = plt.gca()
#     ax.text(
#         0,
#         0.2,
#         label,
#         fontweight="bold",
#         color=color,
#         ha="left",
#         va="center",
#         transform=ax.transAxes,
#     )


# g.map(label, "household_size")

# # Set the subplots to overlap
# g.figure.subplots_adjust(hspace=-0.25)

# # Remove axes details that don't play well with overlap
# g.set_titles("")
# g.set(yticks=[], ylabel="")
# g.despine(bottom=True, left=True)

# plt.show()
