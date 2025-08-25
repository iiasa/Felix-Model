# -*- coding: utf-8 -*-
"""
Created: Thur 21 August 2025
Description: Scripts to plot average daily calorie intake by diet, gender, age cohort
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
    if "calorie_intake" not in output_data_file_name:
        continue

    logging.info("Load outpout data")
    output_data = pd.read_csv(output_data_file)

    logging.info("Configure basic information")
    age_cohorts = [f"{i*5}-{i*5+4}" for i in range(20)] + ["100+"]
    diets = list(np.unique(output_data["diet"]))
    food_categories = list(np.unique(output_data["food_category"]))
    genders = ["female", "male"]

    logging.info("Configure plot years")
    years = ["2023", "2050", "2100"]
    output_data = output_data[["diet", "food_category", "gender", "age"] + years]
    output_data_groups = output_data.groupby(["diet", "gender", "age"])
    output_data_tot = []
    for diet in diets:
        for gender in genders:
            for age_cohort in age_cohorts:
                temp_output_data = output_data_groups.get_group(
                    (diet, gender, age_cohort)
                )

                entry = {
                    "diet": diet,
                    "food_category": "total",
                    "gender": gender,
                    "age": age_cohort,
                }
                for year in years:
                    entry[year] = temp_output_data[year].sum()

                output_data_tot.append(entry)
                del entry, temp_output_data, year
    output_data_tot = pd.DataFrame(output_data_tot)

    fig, axes = plt.subplots(1, 2, figsize=(8, 5), sharey=True)
    linewidth = 1.5
    font_size = 7
    linestyles = ["-", "--", ":"]
    colors = ["blue", "red"]

    # --- Function to determine alpha by age ---
    def get_line_alpha(age_cohort: str):
        if "+" in age_cohort:  # 100+
            age = 100
        else:
            age = int(age_cohort.split("-")[0])
        if age < 25:
            return 0.5
        elif age < 65:
            return 0.8
        else:
            return 1.0

    for ax, diet in zip(axes, diets):
        output_data_tot_diet = output_data_tot[output_data_tot["diet"] == diet]

        for year, linestyle in zip(years, linestyles):
            for gender in genders:
                # Male values negated for left side
                gender_vals = output_data_tot_diet[
                    output_data_tot_diet["gender"] == gender
                ][year].values

                if gender == "male":
                    ax.plot(
                        -gender_vals,
                        age_cohorts,
                        linestyle,
                        color="blue",
                    )
                elif gender == "female":
                    ax.plot(
                        gender_vals * 100,
                        age_cohorts,
                        linestyle,
                        color="black",
                        label=year,
                    )
                    ax.plot(gender_vals, age_cohorts, linestyle, color="red")

        # Formatting
        ax.axvline(0, color="black", linewidth=linewidth)
        ax.set_xlabel("kcal per person")
        ax.set_ylabel("Age Cohort")
        ax.set_title(diet.capitalize())

        # Relabel x-axis ticks as positive values
        xticks = [i for i in range(-4000, 4001, 1000)]
        ax.set_xticks(xticks)
        ax.set_xticklabels([abs(int(x)) for x in xticks])

        max_val = 4000
        ax.set_xlim(-max_val, max_val)

        # --- Add "Male" and "Female" text labels ---
        ax.text(
            -max_val * 0.3,
            age_cohorts[-1],
            "Male",
            fontsize=font_size,
            ha="center",
            va="bottom",
            color="black",
            fontweight="bold",
        )
        ax.text(
            max_val * 0.3,
            age_cohorts[-1],
            "Female",
            fontsize=font_size,
            ha="center",
            va="bottom",
            color="black",
            fontweight="bold",
        )

        # --- Add horizontal lines at ages 25-29 and 65-69 ---
        for target_age in ["24-25", "65-69"]:
            ax.axhline(y=target_age, color="gray", linestyle="--", alpha=0.7)

        ax.legend(loc="lower right", fontsize=font_size)
        del output_data_tot_diet

    plt.tight_layout()
    plt.savefig(
        path_data_output / f"{output_data_file_name.split('.')[0]}.png",
        dpi=600,
        bbox_inches="tight",
    )
    plt.close()
