"""
Created: Wed 18 March 2026
Description: Scripts to plot sensivity analysis of dietary changes
Scope: FeliX model regionalization, module working_paper
Author: Quanliang Ye
Institution: IIASA
Email: yequanliang@iiasa.ac.at
"""

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import datetime
import logging
from pathlib import Path
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os
import re


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
path_data_output = Path(
    "C:/Users/yequanliang/OneDrive - IIASA/research_iiasa/choices/2025.11 diet_scenario_felix_r5/felix_r5/sensitivity_analysis"
)

for output_data_file in path_data_output.glob("felix3_reg_diet_sensi*.csv"):
    output_data_file_name = output_data_file.name

    # Load data
    df = pd.read_csv(output_data_file)

    # Rename first column (assuming it's unnamed)
    df = df.rename(columns={df.columns[0]: "info"})

    # --- Parse the first column ---
    def parse_info(x):
        # Example: "T16 Percentage of vegetarian diet followers[Africa]"
        time_match = re.search(r"T(\d+)", x)
        region_match = re.search(r"\[(.*?)\]", x)

        time = int(time_match.group(1)) if time_match else None
        region = region_match.group(1) if region_match else None

        # remove time + region to isolate variable name
        var = re.sub(r"T\d+\s*", "", x)
        var = re.sub(r"\[.*?\]", "", var).strip()

        return pd.Series([time, var, region])

    df[["time", "variable", "region"]] = df["info"].apply(parse_info)

    # --- Filter for the variable of interest ---
    target_var = "Percentage of vegetarian diet followers"
    df = df[df["variable"] == target_var]

    # --- Filter time range ---
    df = df[(df["time"] >= 100) & (df["time"] <= 200)]

    # --- Extract simulation values (columns 2 onward) ---
    value_cols = df.columns[1:-3]  # exclude info, time, variable, region

    # --- Compute statistics ---
    stats = df.copy()
    stats["median"] = df[value_cols].median(axis=1) * 100
    # stats["std"] = df[value_cols].std(axis=1)*100
    stats["p25"] = df[value_cols].quantile(0.05, axis=1) * 100
    stats["p75"] = df[value_cols].quantile(0.95, axis=1) * 100

    # --- Plot ---
    font_size = 7
    line_width = 1.2

    fig, ax = plt.subplots(figsize=(1.8, 1.4))
    regions = stats["region"].unique()
    region_handles = []
    for region in regions:
        sub = stats[stats["region"] == region].sort_values("time")

        (line,) = ax.plot(sub["time"], sub["median"], label=region)
        ax.fill_between(sub["time"], sub["p25"], sub["p75"], alpha=0.2)
        ax.grid(False)
        # store matching legend handle
        if region == "WestEu":
            region_handles.append(
                Line2D([0], [0], color=line.get_color(), lw=2, label="WestEu_Dev")
            )
        else:
            region_handles.append(
                Line2D([0], [0], color=line.get_color(), lw=2, label=region)
            )
    # Custom legend
    # legend_elements = [
    #     Patch(facecolor="gray", alpha=0.2, label="25–75% range"),
    # ]
    # ax.legend(
    #     handles=region_handles + legend_elements,
    #     loc="center left",
    #     bbox_to_anchor=(1.02, 0.5),  # push outside right
    #     fontsize=font_size,
    #     borderaxespad=0,
    #     facecolor="none",
    #     edgecolor="none",
    # )

    # plt.xlabel("Time")
    # plt.ylabel("Percentage of vegetarian diet followers")
    plt.title(
        " ".join(output_data_file_name.split(".")[0].split("_")[4:]), fontsize=font_size
    )
    ax.set_xlim(115, 200)
    xticks = [115, 150, 200]
    # Convert to years
    xlabels = [t + 1900 for t in xticks]
    plt.xticks(xticks, xlabels, fontsize=font_size)

    yticks = [20, 22, 24, 26, 28]
    ylabels = [f"{y_tick_}%" for y_tick_ in yticks]
    plt.yticks(yticks, ylabels, fontsize=font_size)
    ax.tick_params(axis="both", labelsize=7)

    # plt.xlabel("Year")

    # plt.grid()
    plt.tight_layout()
    # plt.show()
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}_legend.svg",
        dpi=1200,
        # transparent=True,
        bbox_inches="tight",
        pad_inches=0.01,
        format="svg",
    )
    plt.close()
    exit()
