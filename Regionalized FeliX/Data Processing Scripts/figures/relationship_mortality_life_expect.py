# -*- coding: utf-8 -*-
"""
Created: Tue 02 September 2025
Description: Scripts to plot relationships between parameters
Scope: FeliX model regionalization, module working_paper
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""
import matplotlib.pyplot as plt

import datetime
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read the variable
data_home = Path(os.getenv("DATA_HOME"))
current_version = os.getenv(f"CURRENT_VERSION_FELIX_REGIONALIZATION")

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
current_project = "felix_regionalization"
current_module = "working_paper"

logging.info("Configure paths")
path_data_output = (
    data_home / "output_data" / current_project / current_version / current_module
)

for output_data_file in path_data_output.glob("*.csv"):
    output_data_file_name = output_data_file.name
    if "life_expect" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data = pd.read_csv(output_data_file)
    output_data_melt = output_data.melt(
        id_vars=["Parameter", "region", "gender", "age_cohort"],
        var_name="year",
        value_name="value",
    )  # Melt into long format

    output_mortality_pivot = output_data_melt.pivot_table(
        index=["region", "gender", "age_cohort", "year"],
        columns="Parameter",
        values="value",
    ).reset_index()  # Pivot into wide format
    output_life_expect_pivot = (
        output_data_melt[output_data_melt["Parameter"] == "Life Expectancy at Birth"]
        .pivot_table(
            index=["region", "year"],
            columns="Parameter",
            values="value",
        )
        .reset_index()
    )
    output_mortality_pivot["year"] = pd.to_numeric(
        output_mortality_pivot["year"], errors="coerce"
    )  # Convert Year to numeric
    output_life_expect_pivot["year"] = pd.to_numeric(
        output_life_expect_pivot["year"], errors="coerce"
    )  # Convert Year to numeric

    logging.info("Get dimensions")
    regions = output_mortality_pivot["region"].unique()
    genders = output_mortality_pivot["gender"].unique()
    age_cohorts = output_mortality_pivot["age_cohort"].unique()

    ref_life_expect_values = [15, 20, 34, 26, 50]

    logistic_values = {
        "Africa": {
            "female - 0-4": [-0.00067, 0.065626, -1.75474, 2.944219],
            "male - 0-4": [0.000185, 0.070521, -1.75486, 2.923775],
            "female - 10-14": [-0.00264, 0.021804, -0.55467, 1.649569],
            "male - 10-14": [-0.00092, 0.010765, -0.87342, 2.927214],
            "female - 20-24": [-0.01825, 0.081929, -0.17198, -2.0276],
            "male - 20-24": [-0.00042, 0.012324, -1.16881, 3.60782],
            "female - 40-44": [-3.78098, 7.618708, -0.00215, -1.39151],
            "male - 40-44": [-3.76694, 7.596059, -0.00242, -1.01452],
            "female - 60-64": [-21.1949, 21.25454, -0.26154, 28.13159],
            "male - 60-64": [-19.1937, 19.24835, -0.40383, 20.31896],
        },
        "AsiaPacific": {
            "female - 0-4": [-0.00185, 0.072725, -1.69524, 1.996399],
            "male - 0-4": [-0.00148, 0.066233, -1.79542, 2.094574],
            "female - 10-14": [-0.00027, 0.015169, -1.52942, 1.706369],
            "male - 10-14": [-0.00041, 0.016795, -1.27465, 1.498824],
            "female - 20-24": [-0.00026, 0.011881, -1.66597, 2.311001],
            "male - 20-24": [-0.00069, 100.8441, -0.9333, -7.95227],
            "female - 40-44": [0.000219, 0.017206, -1.88594, 2.463275],
            "male - 40-44": [-0.00203, 703.7197, -0.75453, -12.0271],
            "female - 60-64": [-0.00571, 0.067594, -1.13687, 2.637835],
            "male - 60-64": [-0.08417, 279.1206, -0.19205, -37.5868],
        },
        "EastEu": {
            "female - 0-4": [0.000113, 0.006082, -16.4068, 1.960226],
            "male - 0-4": [0.000108, 0.006148, -16.0739, 1.977872],
            "female - 10-14": [2.90e-05, 0.000996, -10.7001, 1.962185],
            "male - 10-14": [4.25e-05, 0.00127, -11.7484, 2.01234],
            "female - 20-24": [8.01e-05, 0.002171, -7.92042, 1.931943],
            "male - 20-24": [2.44e-04, 0.003336, -10.364, 2.083699],
            "female - 40-44": [0.000125, 0.00594, -4.69511, 1.960998],
            "male - 40-44": [0.001432, 0.006452, -10.8178, 2.242394],
            "female - 60-64": [-0.02579, 125.029, -0.62094, -10.9605],
            "male - 60-64": [0.006171, 0.025384, -12.2408, 2.309128],
        },
        "LAC": {
            "female - 0-4": [-0.00013, 0.026079, -4.23698, 2.110785],
            "male - 0-4": [-0.00018, 0.029239, -4.174, 2.125466],
            "female - 10-14": [-7.48e-05, 0.004811, -2.81078, 1.998999],
            "male - 10-14": [-1.80e-04, 0.004301, -2.58583, 2.192111],
            "female - 20-24": [-0.00013, 0.008242, -2.52921, 1.983464],
            "male - 20-24": [-0.02342, 0.068166, -0.25079, 0.805537],
            "female - 40-44": [-0.00059, 0.013337, -2.06983, 2.162628],
            "male - 40-44": [-0.48636, 0.556585, -0.10756, 21.44716],
            "female - 60-64": [-0.01252, 0.055953, -1.26717, 2.533176],
            "male - 60-64": [-0.13815, 0.191709, -0.76903, 4.709756],
        },
        "WestEu_Dev": {
            "female - 0-4": [5.72e-05, 192.1617, -11.857, 0.390858],
            "male - 0-4": [5.57e-05, 218.1724, -10.8861, 0.304317],
            "female - 10-14": [-7.40e-06, 59.09056, -5.89114, -0.63861],
            "male - 10-14": [7.38e-06, 0.001252, -8.96624, 1.372232],
            "female - 20-24": [8.07e-05, 87.04423, -5.25381, -0.84373],
            "male - 20-24": [2.22e-04, 0.001745, -10.3109, 1.572343],
            "female - 40-44": [-0.0003, 152.4765, -3.21215, -1.9965],
            "male - 40-44": [0.000166, 0.005268, -7.62727, 1.531662],
            "female - 60-64": [-0.00568, 272.5322, -2.27176, -2.80428],
            "male - 60-64": [0.002841, 0.028211, -11.3185, 1.537202],
        },
    }

    linewidth = 1.5
    font_size = 7
    colormaps = [
        "#fba686",
        "#c79139",
        "#7d8235",
        "#3f6e55",
        "#165261",
    ]
    markers = ["o", "s", "^", "D", "x"]
    marker_size = 20

    fig, axes = plt.subplots(5, 2, figsize=(6.8, 8.5))
    for i, region in enumerate(regions):
        output_life_expect_pivot_region = output_life_expect_pivot[
            output_life_expect_pivot["region"] == region
        ]

        x_data_life_expect = (
            output_life_expect_pivot_region["Life Expectancy at Birth"]
            / ref_life_expect_values[i]
        )

        def logistic(x: float, L0: float, L: float, k: float, x0: float):
            return L0 + (L / (1 + np.exp(-k * (x - x0))))

        # Row 1: female
        for g, gender in enumerate(genders):
            ax1 = axes[i, g]
            for j, age_cohort in enumerate(age_cohorts):
                L0, L, k, x0 = logistic_values[region][f"{gender} - {age_cohort}"]

                output_mortality_pivot_ = output_mortality_pivot[
                    (output_mortality_pivot["region"] == region)
                    & (output_mortality_pivot["gender"] == gender)
                    & (output_mortality_pivot["age_cohort"] == age_cohort)
                ]["Mortality fraction"]

                # --- Generate smooth curve for regression line ---
                x_fit_life_expect = np.linspace(
                    np.nanmin(x_data_life_expect), np.nanmax(x_data_life_expect), 200
                )
                y_fit_mortality = logistic(x_fit_life_expect, L0, L, k, x0)
                if i == 4 and g == 0:
                    ax1.scatter(
                        x_data_life_expect,
                        output_mortality_pivot_,
                        color=colormaps[j],
                        marker=markers[j],
                        label=age_cohort,
                        alpha=0.5,
                        s=marker_size,
                    )
                else:
                    ax1.scatter(
                        x_data_life_expect,
                        output_mortality_pivot_,
                        color=colormaps[j],
                        marker=markers[j],
                        alpha=0.5,
                        s=marker_size,
                    )
                ax1.plot(
                    x_fit_life_expect,
                    y_fit_mortality,
                    color=colormaps[j],
                    linewidth=linewidth,
                )
                # del L0, L, k, x0, x_fit_gdp, y_fit_gdp
            if i == 0:
                ax1.set_title(gender.capitalize(), fontsize=font_size)
            if i == 4:
                ax1.set_xlabel(r"$\mathit{LE / LE_{1900}}$", fontsize=font_size)
            ax1.tick_params(axis="x", labelsize=font_size)
            ax1.tick_params(axis="y", labelsize=font_size)
            if g == 0:
                ax1.set_ylabel(r"$\mathit{Mort}$" + f" in {region}", fontsize=font_size)

    # Global legend outside bottom
    # handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        # handles,
        # labels,
        loc="lower center",
        ncol=5,
        bbox_to_anchor=(0.5, 0.03),
        fontsize=font_size,
        facecolor="none",
        edgecolor="none",
    )

    plt.subplots_adjust(wspace=0.2, hspace=0.25)
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}.png",
        dpi=600,
        bbox_inches="tight",
    )
    plt.close()
