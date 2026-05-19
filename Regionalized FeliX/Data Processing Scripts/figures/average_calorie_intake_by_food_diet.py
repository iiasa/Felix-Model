# -*- coding: utf-8 -*-
"""
Created: Wed 11 March 2026
Description: Scripts to plot average daily calorie intake by food category, diet, age cohort
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
    if "food_category_diet_scenario" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data = pd.read_csv(output_data_file)
    output_data["age"] = output_data["age"].str.replace('"', "", regex=False)

    logging.info("Configure basic information")
    age_cohorts = [f"{i*5}-{i*5+4}" for i in range(20)] + ["100+"]
    output_data["age"] = pd.Categorical(
        output_data["age"], categories=age_cohorts, ordered=True
    )
    output_data = output_data.sort_values(by=["age"])

    diets = list(np.unique(output_data["diet"]))
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
    scenarios = list(np.unique(output_data.scenario))

    logging.info("Configure plot years")
    years = ["2023", "2050", "2100"]

    fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(6.9, 8), sharey=True)
    linewidth = 1.5
    font_size = 7
    colors = cm.get_cmap("tab10", len(food_categories))
    colormaps = list(reversed([colors(i) for i in range(len(food_categories))]))
    color_map = {
        cat: colormaps[i % len(colormaps)] for i, cat in enumerate(food_categories)
    }

    scenarios_labels = ["FeliX-Reference", "FeliX-Optimistic", "FeliX-Pessimistic"]
    for i, scenario in enumerate(scenarios):
        for j, year in enumerate(years):
            ax = axes[i, j]

            output_data_food = output_data.loc[output_data["scenario"] == scenario][
                ["diet", "food_category", "age", year]
            ]

            logging.info("Pivot to get food categories stacked by gender/age")
            output_data_food_pivot = (
                output_data_food.pivot_table(
                    index=["age", "diet"],
                    columns="food_category",
                    values=year,
                    aggfunc="sum",
                )
                .fillna(0)
                .reset_index()
            )

            output_data_food_pivot["age"] = pd.Categorical(
                output_data_food_pivot["age"], categories=age_cohorts, ordered=True
            )
            del output_data_food

            logging.info("Get data by diet")
            output_data_food_conventional = (
                output_data_food_pivot[output_data_food_pivot["diet"] == "Conventional"]
                .set_index("age")
                .sort_index()
            )
            output_data_food_alternative = (
                output_data_food_pivot[output_data_food_pivot["diet"] == "Alternative"]
                .set_index("age")
                .sort_index()
            )

            output_data_food_conventional = output_data_food_conventional.drop(
                columns="diet", errors="ignore"
            )[food_categories]
            output_data_food_alternative = output_data_food_alternative.drop(
                columns="diet", errors="ignore"
            )[food_categories]

            output_data_food_conventional_cum = output_data_food_conventional.cumsum(
                axis=1
            )
            for col in output_data_food_conventional.columns:
                left = (
                    -output_data_food_conventional_cum[col]
                    + output_data_food_conventional[col]
                )  # left boundary (negative)
                ax.barh(
                    output_data_food_conventional.index,
                    -output_data_food_conventional[col],
                    left=-output_data_food_conventional_cum[col]
                    + output_data_food_conventional[col],
                    label=col if col not in ax.get_legend_handles_labels()[1] else "",
                    alpha=0.8,
                    color=color_map[col],
                )

            output_data_food_alternative_cum = output_data_food_alternative.cumsum(
                axis=1
            )
            for col in output_data_food_alternative.columns:
                left = (
                    output_data_food_alternative_cum[col]
                    - output_data_food_alternative[col]
                )
                ax.barh(
                    output_data_food_alternative.index,
                    output_data_food_alternative[col],
                    left=left,
                    label=col if col not in ax.get_legend_handles_labels()[1] else "",
                    alpha=0.8,
                    color=color_map[col],
                )

            ax.axvline(0, color="black", linewidth=0.8)  # center line
            if int(year) < 2050:
                ax.set_ylabel("Age cohorts", fontsize=font_size)
            ax.tick_params(axis="y", labelsize=7)
            ax.set_title(
                f"{scenarios_labels[i]}, {year}",
                fontsize=font_size,
                fontweight="bold",
            )

            # Relabel x-axis ticks as positive values
            xticks = [i for i in range(-4000, 4001, 2000)]
            ax.set_xticks(xticks)
            ax.set_xticklabels([abs(int(x)) for x in xticks], fontsize=font_size)

            max_val = 4000
            ax.set_xlim(-max_val, max_val)

            ax.set_ylim(-0.5, 22)

            # --- Add "Male" and "Female" text labels ---
            ax.text(
                -max_val * 0.5,
                20.5,
                "Conventional",
                fontsize=font_size,
                ha="center",
                va="bottom",
                color="black",
                # fontweight="bold",
            )
            ax.text(
                max_val * 0.6,
                20.5,
                "Alternative",
                fontsize=font_size,
                ha="center",
                va="bottom",
                color="black",
                # fontweight="bold",
            )

        # Common x-axis label
        fig.text(0.53, 0.005, "kcal per person", ha="center", fontsize=font_size)

    # Global legend outside bottom
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=8,
        bbox_to_anchor=(0.5, -0.03),
        fontsize=font_size,
        facecolor="none",
        edgecolor="none",
    )

    plt.tight_layout()
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}.svg",
        dpi=1200,
        transparent=True,
        bbox_inches="tight",
        pad_inches=0.01,
        format="svg",
    )
    plt.close()
