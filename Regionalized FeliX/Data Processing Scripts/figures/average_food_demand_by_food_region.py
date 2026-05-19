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
    if "food_demand_tonnes_category_region" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data_ = pd.read_csv(output_data_file)
    # output_data_["age"] = output_data_["age"].str.replace('"', "", regex=False)

    scenario_order = list(np.unique(output_data_["scenario"]))
    regions = list(np.unique(output_data_["region"]))

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
    categories = list(reversed(food_categories))

    year = "2050"
    for indicator_ in ["per_capita_kcal", "per_capita", "total"]:
        output_data = output_data_[output_data_["indicator"] == indicator_]

        logging.info("Build stacked data for each bar")
        bars = []
        labels = []
        for scen in scenario_order:
            for region in regions:
                subset = output_data[
                    (output_data["scenario"] == scen)
                    & (output_data["region"] == region)
                ]
                values = subset.set_index("food_category")["2050"].reindex(categories)
                if indicator_ == "per_capita":
                    bars.append(values.values * 1000000 / 365)  # unit in g per day
                elif indicator_ == "total":
                    bars.append(values.values / 1000 / 365)  # unit in thousand tonnes
                elif indicator_ == "per_capita_kcal":
                    bars.append(values.values)  # unit in kcal
                labels.append(f"{scen.split('_')[1]}-{region}")
        bars = np.array(bars)

        font_size = 7
        linewidth = 0.25
        colors = cm.get_cmap("tab10", len(food_categories))
        colormaps = list(reversed([colors(i) for i in range(len(food_categories))]))
        color_map = {
            cat: colormaps[i % len(colormaps)] for i, cat in enumerate(food_categories)
        }

        fig, ax = plt.subplots(figsize=(6, 2.5))
        bottom = np.zeros(len(bars))
        xticks = [
            0.5,
            0.75,
            1,
            1.25,
            1.5,
            2.5,
            2.75,
            3,
            3.25,
            3.5,
            4.5,
            4.75,
            5,
            5.25,
            5.5,
            6.5,
            6.75,
            7,
            7.25,
            7.5,
        ]
        for i, cat in enumerate(categories):
            ax.bar(
                xticks,
                bars[:, i],
                bottom=bottom,
                label=cat,
                width=0.2,
                color=color_map[cat],
                edgecolor="black",
                linewidth=linewidth,
            )
            bottom += bars[:, i]
        for x_pos, xtick_ in enumerate(xticks):
            if x_pos in range(0, 16, 5):
                ax.text(
                    xtick_,
                    bottom[x_pos] + 50,
                    "Africa",
                    fontsize=font_size,
                    ha="center",
                    va="bottom",
                    color="black",
                    # fontweight="bold",
                    rotation=90,
                )
            elif x_pos in range(1, 17, 5):
                if indicator_ == "total":
                    ax.text(
                        xtick_ + 0.25,
                        bottom[x_pos] - 2500,
                        "AsiaPacific",
                        fontsize=font_size,
                        ha="center",
                        va="bottom",
                        color="black",
                        # fontweight="bold",
                        rotation=90,
                    )
                else:
                    ax.text(
                        xtick_,
                        bottom[x_pos] + 50,
                        "AsiaPacific",
                        fontsize=font_size,
                        ha="center",
                        va="bottom",
                        color="black",
                        # fontweight="bold",
                        rotation=90,
                    )
            elif x_pos in range(2, 18, 5):
                ax.text(
                    xtick_,
                    bottom[x_pos] + 50,
                    "EastEu",
                    fontsize=font_size,
                    ha="center",
                    va="bottom",
                    color="black",
                    # fontweight="bold",
                    rotation=90,
                )
            elif x_pos in range(3, 19, 5):
                ax.text(
                    xtick_,
                    bottom[x_pos] + 50,
                    "LAC",
                    fontsize=font_size,
                    ha="center",
                    va="bottom",
                    color="black",
                    # fontweight="bold",
                    rotation=90,
                )
            else:
                if indicator_ in ["per_capita", "per_capita_kcal"]:
                    ax.text(
                        xtick_ + 0.25,
                        bottom[x_pos] - 600,
                        "WestEu_Dev",
                        fontsize=font_size,
                        ha="center",
                        va="bottom",
                        color="black",
                        # fontweight="bold",
                        rotation=90,
                    )

                else:
                    ax.text(
                        xtick_,
                        bottom[x_pos] + 50,
                        "WestEu_Dev",
                        fontsize=font_size,
                        ha="center",
                        va="bottom",
                        color="black",
                        # fontweight="bold",
                        rotation=90,
                    )

        logging.info("Axis formatting")

        ax.set_xticks([1, 3, 5, 7])
        ax.set_xticklabels(
            scenario_order,
            fontsize=font_size,
        )
        if indicator_ == "per_capita":
            ax.set_title(
                f"Daily food demand per capita, {year}",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_yticks(range(0, 3201, 800))
            ax.set_ylabel("Unit in g", fontsize=font_size)
        elif indicator_ == "total":
            ax.set_title(
                f"Daily food demand total, {year}",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_yticks(range(0, 10001, 2000))
            ax.set_ylabel("Unit in thousand tonnes", fontsize=font_size)
        elif indicator_ == "per_capita_kcal":
            ax.set_title(
                f"Daily calorie demand per capita, {year}",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_yticks(range(0, 4401, 1100))
            ax.set_ylabel("Unit in kcal", fontsize=font_size)
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
        # plt.show()
        # exit()
        plt.savefig(
            path_data_output
            / f"{output_data_file_name.split('.')[0]}_{indicator_}.svg",
            dpi=1200,
            # transparent=True,
            bbox_inches="tight",
            pad_inches=0.01,
            format="svg",
        )
        plt.close()
