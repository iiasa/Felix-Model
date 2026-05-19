# -*- coding: utf-8 -*-
"""
Created: Thur 12 March 2026
Description: Scripts to plot average food demand by food category under different scenarios
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
    if "food_demand_tonnes_category" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data_ = pd.read_csv(output_data_file)

    scenario_order = ["FeliX_ref", "FeliX_optimi", "FeliX_pessi"]
    # Food category order (keeps stacks consistent)
    food_categories = [
        "PasMeat",
        "CropMeat",
        "Dairy",
        "Eggs",
        "Pulses",
        "Grains",
        "VegFruits",
        "OtherCrops",
    ]
    categories = [food_categories[-pos - 1] for pos in range(0, len(food_categories))]

    for indicator_ in ["per_capita", "total"]:
        output_data = output_data_[output_data_["indicator"] == indicator_]

        logging.info("Build stacked data for each bar")
        bars = []
        labels = []
        for scen in scenario_order:
            for year in ["2050", "2100"]:
                subset = output_data[output_data["scenario"] == scen]
                values = subset.set_index("food_category")[year].reindex(categories)
                if indicator_ == "per_capita":
                    bars.append(values.values * 1000000 / 365)  # unit in g per day
                else:
                    bars.append(values.values / 1000 / 365)  # unit in thousand tonnes
                labels.append(f"{scen.split('_')[1]}-{year}")
        bars = np.array(bars)

        font_size = 7
        linewidth = 0.25
        colors = cm.get_cmap("tab10", len(food_categories))
        colormaps = list(reversed([colors(i) for i in range(len(food_categories))]))
        color_map = {
            cat: colormaps[i % len(colormaps)] for i, cat in enumerate(food_categories)
        }

        fig, ax = plt.subplots(figsize=(5, 2.5))
        bottom = np.zeros(len(bars))
        xticks = [0.75, 1.25, 2.25, 2.75, 3.75, 4.25]
        for i, cat in enumerate(categories):
            ax.bar(
                xticks,
                bars[:, i],
                bottom=bottom,
                label=cat,
                width=0.4,
                color=color_map[cat],
                edgecolor="black",
                linewidth=linewidth,
            )
            bottom += bars[:, i]
        for x_pos, xtick_ in enumerate(xticks):
            if x_pos in [0, 2, 4]:
                ax.text(
                    xtick_,
                    bottom[x_pos] + 10,
                    "2050",
                    fontsize=font_size,
                    ha="center",
                    va="bottom",
                    color="black",
                    # fontweight="bold",
                )
            else:
                ax.text(
                    xtick_,
                    bottom[x_pos] + 10,
                    "2100",
                    fontsize=font_size,
                    ha="center",
                    va="bottom",
                    color="black",
                    # fontweight="bold",
                )

        logging.info("Axis formatting")

        ax.set_xticks([1, 2.5, 4])
        ax.set_xticklabels(
            ["FeliX-Reference", "FeliX-Optimistic", "FeliX-Pessimistic"],
            fontsize=font_size,
        )
        if indicator_ == "per_capita":
            ax.set_title(
                "Daily food demand per capita",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_yticks(range(0, 2501, 500))
            ax.set_ylabel("Unit in g", fontsize=font_size)
        else:
            ax.set_title(
                "Daily food demand total",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_yticks(range(0, 35001, 7000))
            ax.set_ylabel("Unit in thousand tonnes", fontsize=font_size)
        ax.tick_params(axis="both", labelsize=7)

        ax.legend(
            # title="Food Category",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            fontsize=font_size,
            facecolor="none",
            edgecolor="none",
        )

        plt.tight_layout()
        plt.savefig(
            path_data_output
            / f"{output_data_file_name.split('.')[0]}_{indicator_}.svg",
            dpi=1200,
            transparent=True,
            bbox_inches="tight",
            pad_inches=0.01,
            format="svg",
        )
        plt.close()
