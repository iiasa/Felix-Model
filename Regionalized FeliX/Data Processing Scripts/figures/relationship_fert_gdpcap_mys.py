# -*- coding: utf-8 -*-
"""
Created: Mon 01 September 2025
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
    if "total_fertility" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data = pd.read_csv(output_data_file)
    output_data_melt = output_data.melt(
        id_vars=["Parameter", "region"], var_name="year", value_name="value"
    )  # Melt into long format
    output_data_pivot = output_data_melt.pivot_table(
        index=["region", "year"], columns="Parameter", values="value"
    ).reset_index()  # Pivot into wide format
    output_data_pivot["year"] = pd.to_numeric(
        output_data_pivot["year"], errors="coerce"
    )  # Convert Year to numeric

    logging.info("Get region dimension")
    regions = output_data_pivot["region"].unique()

    ref_values = {
        "Total Fertility": [5.19, 2.559, 1.287, 2.61, 1.743],
        "GDP per Capita": [1273.36, 2677.24, 4220.4, 5514.78, 34356.4],
        "Mean Years of Schooling": [4.74735, 6.69995, 9.83956, 7.25129, 11.2275],
    }

    logistic_values = {
        "GDP per Capita": {
            "Africa": [
                0.802667,
                0.0941985,
                19.3124,
                0.999919,
            ],
            "AsiaPacific": [
                1.01575,
                0.581,
                -3.29288,
                1.33779,
            ],
            "EastEu": [
                1.00145,
                1.45425,
                2.11906,
                2.26138,
            ],
            "LAC": [
                0.414658,
                0.350105,
                -8.57358,
                0.920609,
            ],
            "WestEu_Dev": [
                1.10355,
                0.0615668,
                -100,
                1.26764,
            ],
        },
        "Mean Years of Schooling": {
            "Africa": [
                0.360769,
                1.44416,
                -2.44897,
                1.3873,
            ],
            "AsiaPacific": [
                0.721517,
                0.875958,
                -13.0561,
                0.555801,
            ],
            "EastEu": [
                0.0175774,
                3.97607,
                -2.65641,
                0.588429,
            ],
            "LAC": [
                1.57607,
                1.47316,
                -13.6822,
                0.698973,
            ],
            "WestEu_Dev": [
                0.861392,
                0.545073,
                -41.1206,
                0.672077,
            ],
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

    whether_reference = False
    if whether_reference:
        fig, axes = plt.subplots(3, 5, figsize=(7, 5))
        for i, region in enumerate(regions):
            output_data_pivot_region = output_data_pivot[
                output_data_pivot["region"] == region
            ]

            x_data_gdp = (
                output_data_pivot_region["GDP per Capita"]
                / ref_values["GDP per Capita"][i]
            )
            x_data_mys = (
                output_data_pivot_region["Mean Years of Schooling"]
                / ref_values["Mean Years of Schooling"][i]
            )
            y_data_fert = (
                output_data_pivot_region["Total Fertility"]
                / ref_values["Total Fertility"][i]
            )
            logging.info("Define logestic function")

            def logistic(x: float, L0: float, L: float, k: float, x0: float):
                return L0 + (L / (1 + np.exp(-k * (x - x0))))

            # Row 1: Fertility vs GDP
            ax1 = axes[0, i]
            L0, L, k, x0 = logistic_values["GDP per Capita"][region]
            # --- Generate smooth curve for regression line ---
            x_fit_gdp = np.linspace(np.nanmin(x_data_gdp), np.nanmax(x_data_gdp), 200)
            y_fit_gdp = logistic(x_fit_gdp, L0, L, k, x0)
            ax1.scatter(
                x_data_gdp, y_data_fert, color=colormaps[i], alpha=0.7, label="Data"
            )
            ax1.plot(x_fit_gdp, y_fit_gdp, color=colormaps[i], label="Simulated")
            # del L0, L, k, x0, x_fit_gdp, y_fit_gdp

            ax1.set_title(region, fontsize=font_size)
            if i == 2:
                ax1.set_xlabel("GDP_per_Cap / GDP_per_Cap_2000", fontsize=font_size)
            ax1.tick_params(axis="x", labelsize=font_size)
            ax1.tick_params(axis="y", labelsize=font_size)
            if i == 0:
                ax1.set_ylabel("Tot_Fert / Norm_Fert", fontsize=font_size)

            # Row 2: Fertility vs MYS
            ax2 = axes[1, i]
            L0, L, k, x0 = logistic_values["Mean Years of Schooling"][region]
            # --- Generate smooth curve for regression line ---
            x_fit_mys = np.linspace(min(x_data_mys), max(x_data_mys), 200)
            y_fit_mys = logistic(x_fit_mys, L0, L, k, x0)

            ax2.scatter(
                x_data_mys, y_data_fert, color=colormaps[i], alpha=0.7, label="Data"
            )
            ax2.plot(x_fit_mys, y_fit_mys, color=colormaps[i], label="Simulated")
            ax2.set_title(region, fontsize=font_size)
            if i == 2:
                ax2.set_xlabel("MYS / MYS_2000", fontsize=font_size)
            ax2.tick_params(axis="x", labelsize=font_size)
            ax2.tick_params(axis="y", labelsize=font_size)
            if i == 0:
                ax2.set_ylabel("Tot_Fert / Norm_Fert", fontsize=font_size)

            # Row 3: Fertility data vs simulated data
            ax3 = axes[2, i]
            ax3.plot(
                output_data_pivot_region["year"],
                output_data_pivot_region["Total Fertility"],
                color=colormaps[i],
                alpha=0.7,
                label="Simulated",
                linewidth=linewidth,
            )
            ax3.plot(
                output_data_pivot_region["year"],
                output_data_pivot_region["Total Fertility Modeled"],
                "--",
                color=colormaps[i],
                label="Simulated",
            )

            ax3.set_title(region, fontsize=font_size)
            if i == 2:
                ax3.set_xlabel("Year", fontsize=font_size)
            ax3.tick_params(axis="x", labelsize=font_size)
            ax3.tick_params(axis="y", labelsize=font_size)
            if i == 0:
                ax3.set_ylabel("Total fertility", fontsize=font_size)
    else:
        fig, axes = plt.subplots(2, 5, figsize=(7, 3.8))
        for i, region in enumerate(regions):
            output_data_pivot_region = output_data_pivot[
                output_data_pivot["region"] == region
            ]

            # Row 1: Fertility vs GDP
            ax1 = axes[0, i]
            ax1.scatter(
                output_data_pivot_region["GDP per Capita"],
                output_data_pivot_region["Total Fertility"],
                color=colormaps[i],
                alpha=0.7,
            )
            ax1.set_title(region, fontsize=font_size)
            if i == 2:
                ax1.set_xlabel("GDP per capita (2005 US $)", fontsize=font_size)
            ax1.tick_params(axis="x", labelsize=font_size)
            yticks = [i for i in range(0, 8, 2)]
            ax1.set_yticks(yticks)
            ax1.set_yticklabels([abs(int(y_)) for y_ in yticks], fontsize=font_size)
            if i == 0:
                ax1.set_ylabel("Total Fertility", fontsize=font_size)

            # Row 2: Fertility vs MYS
            ax2 = axes[1, i]
            ax2.scatter(
                output_data_pivot_region["Mean Years of Schooling"],
                output_data_pivot_region["Total Fertility"],
                color=colormaps[i],
                alpha=0.7,
            )
            ax2.set_title(region, fontsize=font_size)
            if i == 2:
                ax2.set_xlabel("Mean years of schooling (years)", fontsize=font_size)
            ax2.tick_params(axis="x", labelsize=font_size)
            yticks = [i for i in range(0, 8, 2)]
            ax2.set_yticks(yticks)
            ax2.set_yticklabels([abs(int(y_)) for y_ in yticks], fontsize=font_size)
            if i == 0:
                ax2.set_ylabel("Total Fertility", fontsize=font_size)

    plt.subplots_adjust(wspace=0.3, hspace=0.5)
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}.png",
        dpi=600,
        bbox_inches="tight",
    )
    plt.close()
