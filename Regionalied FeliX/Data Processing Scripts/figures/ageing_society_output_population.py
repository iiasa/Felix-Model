# -*- coding: utf-8 -*-
"""
Created: Tue 8 July 2025
Description: Scripts to plot the future project of key variables
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

    logging.info("Load outpout data by regionalized felix")
    output_data = pd.read_csv(output_data_file, index_col=0, header=[0, 1])
    regions = sorted(list(set(column_[0] for column_ in output_data.columns)))

    logging.info("Configure historic and future years")
    years = np.arange(2000, 2022)  # Historical years
    future_years = np.arange(2021, 2101)  # Future projection years

    logging.info("Configure scenario names")
    sce_names = [column_[1] for column_ in output_data.columns[:4]]

    logging.info("Data processing")
    historical_data = {
        region: np.array(output_data.loc[years, (region, "FeliX")])
        for region in regions
    }

    future_data = {
        region: [
            np.array(output_data.loc[future_years, (region, sce_name)])
            for sce_name in sce_names
        ]
        for region in regions
    }

    logging.info("Create figure and subplots")
    font_size = 7
    colormaps = ["#fcbed2", "#e79a5d", "#7d8235", "#1f5e63"]
    line_width = 2
    # Set global font size to 7
    plt.rcParams.update({"font.size": font_size})

    fig, axes = plt.subplots(1, 5, figsize=(7.5, 1.5), sharey=True)

    for ax, region in zip(axes, regions):
        logging.info("Plot historical data")
        ax.plot(
            years,
            historical_data[region],
            color="black",
            label="Historical",
            linewidth=line_width,
            linestyle="-",
        )

        logging.info("Plot future projections")
        for i, projection in enumerate(future_data[region]):
            if i == 0:
                label_ = "BAU"
            else:
                label_ = sce_names[i][6:]
            ax.plot(
                future_years,
                projection,
                linestyle="--",
                color=colormaps[i],
                linewidth=line_width,
                label=label_,
                alpha=0.7,
            )
            ax.tick_params(axis="x", labelrotation=90)
            del label_

        ax.set_title(region, fontsize=font_size, fontweight="bold")

        ax.set_xlim(years[0], future_years[-1])

    # Add y-axis label to first subplot
    # axes[2].set_xlabel("Year")
    axes[0].set_ylabel(
        output_data_file_name.split(".")[0].replace("_", " "), fontweight="bold"
    )

    # Add legend to first subplot only to avoid clutter
    legend = axes[-1].legend(
        loc="center left", bbox_to_anchor=(1.05, 0.5), fontsize=font_size
    )
    legend.set_frame_on(False)

    # plt.tight_layout()
    fig.subplots_adjust(right=0.82)
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}_trends.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    del output_data
