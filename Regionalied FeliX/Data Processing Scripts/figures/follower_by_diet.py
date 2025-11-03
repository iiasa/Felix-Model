# -*- coding: utf-8 -*-
"""
Created: Thur 25 September 2025
Description: Scripts to plot followers by diet, gender, age cohort
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
    if "by_scenario" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data = pd.read_csv(output_data_file)

    logging.info("Configure basic information")
    age_cohorts = [f"{i*5}-{i*5+4}" for i in range(20)] + ["100+"]
    output_data["age"] = pd.Categorical(
        output_data["age"], categories=age_cohorts, ordered=True
    )
    output_data = output_data.sort_values(by=["age"])

    diets = list(reversed(np.unique(output_data["diet"])))
    genders = ["female", "male"]
    scenarios = list(np.unique(output_data.scenario))

    logging.info("Configure plot years")
    years = ["2050", "2100"]
    output_data = output_data[["diet", "gender", "age", "scenario"] + years]

    fig, axes = plt.subplots(2, 2, figsize=(6, 5), sharey=True)
    linewidth = 1.5
    font_size = 7
    linestyles = ["-", "--", ":"]
    colors = ["blue", "red"]

    # Set global font size to 7
    plt.rcParams.update({"font.size": font_size})

    for i, year in enumerate(years):
        for j, diet in enumerate(diets):
            ax = axes[j, i]

            for scenario, linestyle in zip(scenarios, linestyles):
                output_data_tot_diet = output_data.loc[
                    (output_data["diet"] == diet)
                    & (output_data["scenario"] == scenario)
                ][["gender", "age", year]]

                for gender in genders:
                    # Male values negated for left side
                    gender_vals = (
                        output_data_tot_diet[output_data_tot_diet["gender"] == gender][
                            year
                        ].values
                        / 1000000
                    )  # unit in million people

                    if gender == "male":
                        ax.plot(
                            -gender_vals,
                            age_cohorts,
                            linestyle,
                            color="blue",
                        )
                    elif gender == "female":
                        ax.plot(
                            gender_vals * 1000000,
                            age_cohorts,
                            linestyle,
                            color="black",
                            label=scenario,
                        )
                        ax.plot(
                            gender_vals,
                            age_cohorts,
                            linestyle,
                            color="red",
                        )

            # Formatting
            ax.axvline(0, color="black", linewidth=linewidth)
            if j == 1:
                ax.set_xlabel("million person", fontsize=font_size)

            ax.set_title(
                f"{diet}, {year}",
                fontsize=font_size,
                fontweight="bold",
            )
            ax.set_ylabel("Age Cohort", fontsize=font_size)
            ax.tick_params(axis="y", labelsize=7)

            # Relabel x-axis ticks as positive values
            max_val = 800
            xticks = [i for i in range(-max_val, max_val + 1, 200)]
            ax.set_xticks(xticks)
            ax.set_xticklabels([abs(int(x)) for x in xticks], fontsize=font_size)

            ax.set_xlim(-max_val, max_val)

            # --- Add "Male" and "Female" text labels ---
            ax.text(
                -max_val * 0.3,
                age_cohorts[-2],
                "Male",
                fontsize=font_size,
                ha="center",
                va="bottom",
                color="black",
                # fontweight="bold",
            )
            ax.text(
                max_val * 0.3,
                age_cohorts[-2],
                "Female",
                fontsize=font_size,
                ha="center",
                va="bottom",
                color="black",
                # fontweight="bold",
            )

            # --- Add horizontal lines at ages 25-29 and 65-69 ---
            for target_age in ["25-29", "65-69"]:
                ax.axhline(y=target_age, color="gray", linestyle="--", alpha=0.7)

            # ax.legend(
            #     loc="upper right",
            #     fontsize=font_size,
            #     facecolor="none",
            #     edgecolor="none",
            # )
            del output_data_tot_diet
    # Global legend outside bottom
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        bbox_to_anchor=(0.5, -0.03),
        fontsize=font_size,
        facecolor="none",
        edgecolor="none",
    )

    plt.tight_layout()
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}.png",
        dpi=600,
        bbox_inches="tight",
    )
    plt.close()
