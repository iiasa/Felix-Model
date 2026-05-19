# -*- coding: utf-8 -*-
"""
Created: Fri 13 March 2026
Description: Scripts to plot followers by diet, gender, age cohort, and by Region!!!
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
    if "diet_followers_by_region_by_scenario" not in output_data_file_name:
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

    diets = list(reversed(np.unique(output_data["diet"])))
    genders = ["female", "male"]
    scenarios = list(np.unique(output_data.scenario))
    regions = list(np.unique(output_data["region"]))

    logging.info("Configure plot years")
    year = "2050"
    output_data = output_data[["diet", "region", "gender", "age", "scenario", year]]
    output_data = (
        output_data.groupby(["diet", "region", "age", "scenario"])[year]
        .sum()
        .reset_index(name=year)
    )

    fig, axes = plt.subplots(3, 2, figsize=(6, 5), sharey=True)
    axes = axes.flatten()
    linewidth = 1.5
    font_size = 7
    linestyles = ["-", "--", ":", "-."]
    colors = ["blue", "red"]

    # Set global font size to 7
    plt.rcParams.update({"font.size": font_size})
    all_plot_data = pd.DataFrame()

    for j, region in enumerate(regions):
        ax = axes[j]

        for scenario, linestyle in zip(scenarios, linestyles):
            output_data_tot_diet = output_data.loc[
                (output_data["scenario"] == scenario)
                & (output_data["region"] == region)
            ][["diet", "age", year]]

            for diet in diets:
                # Diet values negated for left side
                diet_vals = (
                    output_data_tot_diet[output_data_tot_diet["diet"] == diet][
                        year
                    ].values
                    / 1000000
                )  # unit in million people

                if diet == "Conventional":
                    ax.plot(
                        -diet_vals,
                        age_cohorts,
                        linestyle,
                        color="blue",
                    )
                elif diet == "Alternative":
                    ax.plot(
                        diet_vals * 1000000,
                        age_cohorts,
                        linestyle,
                        color="black",
                        label=scenario,
                    )
                    ax.plot(
                        diet_vals,
                        age_cohorts,
                        linestyle,
                        color="red",
                    )
                all_plot_data[f"{year}_{diet}_{scenario}_{region}"] = diet_vals

        # Formatting
        ax.axvline(0, color="black", linewidth=linewidth)
        if j in [3, 4]:
            ax.set_xlabel("Million persons", fontsize=font_size)

        ax.set_title(
            f"{region}, {year}",
            fontsize=font_size,
            fontweight="bold",
        )
        ax.set_ylabel("Age cohorts", fontsize=font_size)
        ax.tick_params(axis="y", labelsize=7)

        yticks = [i for i in range(0, 21, 2)]
        ax.set_yticks(yticks)
        ax.set_xticklabels([age_cohorts[pos] for pos in yticks], fontsize=font_size)

        # Relabel x-axis ticks as positive values
        max_val = 300
        xticks = [i for i in range(-max_val, max_val + 1, 100)]
        ax.set_xticks(xticks)
        ax.set_xticklabels([abs(int(x)) for x in xticks], fontsize=font_size)

        ax.set_xlim(-max_val, max_val)

        # --- Add "Male" and "Female" text labels ---
        ax.text(
            -max_val * 0.4,
            age_cohorts[-3],
            "Conventional",
            fontsize=font_size,
            ha="center",
            va="bottom",
            color="black",
            # fontweight="bold",
        )
        ax.text(
            max_val * 0.4,
            age_cohorts[-3],
            "Alternative",
            fontsize=font_size,
            ha="center",
            va="bottom",
            color="black",
            # fontweight="bold",
        )

        # --- Add horizontal lines at ages 25-29 and 65-69 ---
        for target_age in ["25-29", "65-69"]:
            ax.axhline(y=target_age, color="gray", linestyle="--", alpha=0.7)
        # if region == "WestEu":
        #     ax.legend(
        #         bbox_to_anchor=(1.02, 1),
        #         loc="upper left",
        #         fontsize=font_size,
        #         facecolor="none",
        #         edgecolor="none",
        #     )
        del output_data_tot_diet
    # remove last axis
    fig.delaxes(axes[5])
    # Global legend outside bottom
    handles, labels = ax.get_legend_handles_labels()
    labels = scenarios
    fig.legend(
        handles,
        labels,
        title="Diet composition scenarios",
        loc="lower center",
        ncol=1,
        bbox_to_anchor=(0.7, 0.15),
        fontsize=font_size,
        facecolor="none",
        edgecolor="none",
    )

    all_plot_data.to_csv(f"{output_data_file_name.split('.')[0]}.csv")
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
