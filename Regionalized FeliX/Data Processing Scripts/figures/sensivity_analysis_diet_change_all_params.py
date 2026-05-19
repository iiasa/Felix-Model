"""
Created: Thur 19 March 2026
Description: Scripts to plot sensivity analysis of dietary changes, combination of parameters
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

df_all = pd.DataFrame()
for output_data_file in path_data_output.glob("felix3_reg_diet_sensi*.csv"):
    logging.info("Load data")
    df = pd.read_csv(output_data_file)
    df = df.rename(columns={df.columns[0]: "info"})

    logging.info("Parse the first column")

    def parse_info(x):
        # Example: "T16 Percentage of vegetarian diet followers[Africa]"
        time_match = re.search(r"T(\d+)", x)
        region_match = re.search(r"\[(.*?)\]", x)

        time = int(time_match.group(1)) if time_match else None
        region = region_match.group(1).split(",")[0] if region_match else None
        try:
            food_category = (
                region_match.group(1).split(",")[1] if region_match else None
            )
        except IndexError:
            food_category = None
        # remove time + region to isolate variable name
        var = re.sub(r"T\d+\s*", "", x)
        var = re.sub(r"\[.*?\]", "", var).strip()

        return pd.Series([time, var, region, food_category])

    df[["time", "variable", "region", "food_category"]] = df["info"].apply(parse_info)
    df["scenario"] = output_data_file.name.split("_")[-1].split(".")[0]
    df_all = pd.concat([df_all, df], ignore_index=True)

    del df, output_data_file


logging.info("Filter for the variable of interest")
target_vars = [
    "Percentage of vegetarian diet followers",
    "Plant Based Food Caloric Intake per Person",
    "Animal Based Food Caloric Intake per Person",
    "Percentage of animal calories",
]
food_categories = list(df_all["food_category"].unique())
food_categories.remove(None)
regions = list(df_all["region"].unique())[:5]
scenarios = df_all["scenario"].unique()
scenario_mapping = {
    "sc0": "Sc0_BAU",
    "sc1": "Sc1_Healthy_BAU",
    "sc2": "Sc2_Healthy_Vegan",
    "sc3": "Sc3_Flexitarian_Vegan",
}

fig_width = 6.4
fig_height = 2.2
by_sceanrio = False

if by_sceanrio:
    logging.info("Target variable is percentage")
    for pos_var, target_var in enumerate(target_vars):
        df_var_food = df_all[(df_all["variable"] == target_var)]
        df_var_food = df_var_food[
            (df_var_food["time"] >= 100) & (df_var_food["time"] <= 200)
        ]

        logging.info("Extract simulation values (columns 2 onward)")
        value_cols = df_var_food.columns[
            1:-5
        ]  # exclude info, time, variable, region, food_category,scenario

        # --- Compute statistics ---
        stats = df_var_food.copy()
        if "Percentage " in target_var:
            stats["median"] = df_var_food[value_cols].median(axis=1) * 100
            stats["p25"] = df_var_food[value_cols].quantile(0.25, axis=1) * 100
            stats["p75"] = df_var_food[value_cols].quantile(0.75, axis=1) * 100
        else:
            stats["median"] = df_var_food[value_cols].median(axis=1)
            stats["p25"] = df_var_food[value_cols].quantile(0.25, axis=1)
            stats["p75"] = df_var_food[value_cols].quantile(0.75, axis=1)

        logging.info("Plot")
        font_size = 7
        line_width = 1.2
        fig, axes = plt.subplots(
            nrows=1, ncols=4, figsize=(fig_width, fig_height), sharey=True
        )
        region_handles = []
        for pos_scenario, scenario_ in enumerate(scenarios):
            ax = axes[pos_scenario]
            for pos_region, region in enumerate(regions):
                sub = stats[
                    (stats["region"] == region) & (stats["scenario"] == scenario_)
                ].sort_values("time")

                (line,) = ax.plot(sub["time"], sub["median"], label=region)
                ax.fill_between(sub["time"], sub["p25"], sub["p75"], alpha=0.2)
                ax.grid(False)
                # store matching legend handle
                if region == "WestEu":
                    region_handles.append(
                        Line2D(
                            [0],
                            [0],
                            color=line.get_color(),
                            lw=2,
                            label="WestEu_Dev",
                        )
                    )
                else:
                    region_handles.append(
                        Line2D([0], [0], color=line.get_color(), lw=2, label=region)
                    )
                # # Custom legend
                # legend_elements = [
                #     Patch(facecolor="gray", alpha=0.2, label="25–75% range"),
                # ]
                # ax.legend(
                #     handles=region_handles + legend_elements,
                #     loc="center left",
                #     bbox_to_anchor=(-0.2, 0.5),  # push outside right
                #     fontsize=font_size,
                #     borderaxespad=0,
                #     facecolor="none",
                #     edgecolor="none",
                #     ncol=6,
                # )

                ax.set_title(
                    scenario_mapping[scenario_], fontsize=font_size, fontweight="bold"
                )
                ax.set_xlim(115, 200)
                xticks = [115, 150, 200]
                # Convert to years
                xlabels = [t + 1900 for t in xticks]
                ax.set_xticks(xticks, xlabels, fontsize=font_size, rotation=90)
                # ax.set_xticks(xticks, [])

                if pos_var == 0:
                    ax.set_ylim(18, 38)
                elif pos_var == 1:
                    ax.set_ylim(1800, 4200)
                elif pos_var == 2:
                    ax.set_ylim(0, 1280)
                elif pos_var == 3:
                    ax.set_ylim(4, 32)
                yticks = [
                    ytick_
                    for ytick_ in range(
                        int(ax.get_ylim()[0]),
                        int(ax.get_ylim()[1]) + 1,
                        int((ax.get_ylim()[1] - ax.get_ylim()[0]) / 4),
                    )
                ]

                if "Percentage " in target_var:
                    ylabels = [f"{y_tick_}%" for y_tick_ in yticks]
                else:
                    ylabels = [y_tick_ for y_tick_ in yticks]
                ax.set_yticks(yticks, ylabels, fontsize=font_size)
                ax.tick_params(axis="both", labelsize=7)
                if pos_scenario == 0:
                    if pos_var == 0:
                        ax.set_ylabel(
                            "Share of alter. diet follower", fontsize=font_size
                        )
                    elif pos_var == 1:
                        ax.set_ylabel(
                            "Daily plant-based cal. intake", fontsize=font_size
                        )
                    elif pos_var == 2:
                        ax.set_ylabel(
                            "Daily animal-based cal. intake", fontsize=font_size
                        )
                    elif pos_var == 3:
                        ax.set_ylabel("Share of animal-based cal.", fontsize=font_size)
                del (sub, pos_region)
        plt.tight_layout(w_pad=0.5)
        plt.savefig(
            path_data_output / f"felix3_reg_diet_sensi_{target_var}_all.svg",
            dpi=1200,
            # transparent=True,
            bbox_inches="tight",
            pad_inches=0.01,
            format="svg",
        )
else:
    logging.info("Plot by region")
    for pos_var, target_var in enumerate(target_vars):
        df_var_food = df_all[(df_all["variable"] == target_var)]
        df_var_food = df_var_food[
            (df_var_food["time"] >= 100) & (df_var_food["time"] <= 200)
        ]

        logging.info("Extract simulation values (columns 2 onward)")
        value_cols = df_var_food.columns[
            1:-5
        ]  # exclude info, time, variable, region, food_category,scenario

        # --- Compute statistics ---
        stats = df_var_food.copy()
        if "Percentage " in target_var:
            stats["median"] = df_var_food[value_cols].median(axis=1) * 100
            stats["p25"] = df_var_food[value_cols].quantile(0.25, axis=1) * 100
            stats["p75"] = df_var_food[value_cols].quantile(0.75, axis=1) * 100
        else:
            stats["median"] = df_var_food[value_cols].median(axis=1)
            stats["p25"] = df_var_food[value_cols].quantile(0.25, axis=1)
            stats["p75"] = df_var_food[value_cols].quantile(0.75, axis=1)

        logging.info("Plot")
        font_size = 7
        line_width = 1.2
        fig, axes = plt.subplots(
            nrows=1, ncols=5, figsize=(fig_width, fig_height), sharey=True
        )
        for pos_region, region in enumerate(regions):
            ax = axes[pos_region]
            scenario_handles = []
            for pos_scenario, scenario_ in enumerate(scenarios):
                sub = stats[
                    (stats["region"] == region) & (stats["scenario"] == scenario_)
                ].sort_values("time")

                (line,) = ax.plot(
                    sub["time"], sub["median"], label=scenario_mapping[scenario_]
                )
                ax.fill_between(sub["time"], sub["p25"], sub["p75"], alpha=0.2)
                ax.grid(False)

                # store matching legend handle
                # scenario_handles.append(
                #     Line2D(
                #         [0],
                #         [0],
                #         color=line.get_color(),
                #         lw=2,
                #         label=scenario_mapping[scenario_],
                #     )
                # )

                # Custom legend
                # legend_elements = [
                #     Patch(facecolor="gray", alpha=0.2, label="25–75% range"),
                # ]
                # if pos_region == 0:
                #     ax.legend(
                #         handles=scenario_handles + legend_elements,
                #         loc="lower left",
                #         bbox_to_anchor=(-0.2, -0.2),  # push outside right
                #         fontsize=font_size,
                #         borderaxespad=0,
                #         facecolor="none",
                #         edgecolor="none",
                #         ncol=6,
                #     )

                if region == "WestEu":
                    ax.set_title("WestEu_Dev", fontsize=font_size, fontweight="bold")
                else:
                    ax.set_title(region, fontsize=font_size, fontweight="bold")
                ax.set_xlim(115, 200)
                xticks = [115, 150, 200]
                # Convert to years
                xlabels = [t + 1900 for t in xticks]
                ax.set_xticks(xticks, xlabels, fontsize=font_size, rotation=90)
                # ax.set_xticks(xticks, [])

                if pos_var == 0:
                    ax.set_ylim(18, 38)
                elif pos_var == 1:
                    ax.set_ylim(1800, 4200)
                elif pos_var == 2:
                    ax.set_ylim(0, 1280)
                elif pos_var == 3:
                    ax.set_ylim(4, 32)
                yticks = [
                    ytick_
                    for ytick_ in range(
                        int(ax.get_ylim()[0]),
                        int(ax.get_ylim()[1]) + 1,
                        int((ax.get_ylim()[1] - ax.get_ylim()[0]) / 4),
                    )
                ]

                if "Percentage " in target_var:
                    ylabels = [f"{y_tick_}%" for y_tick_ in yticks]
                else:
                    ylabels = [y_tick_ for y_tick_ in yticks]
                ax.set_yticks(yticks, ylabels, fontsize=font_size)
                # ax.set_yticks(yticks, [], fontsize=font_size)
                ax.tick_params(axis="both", labelsize=7)
                if pos_scenario == 0:
                    if pos_var == 0:
                        ax.set_ylabel(
                            "Share of alter. diet follower", fontsize=font_size
                        )
                    elif pos_var == 1:
                        ax.set_ylabel(
                            "Daily plant-based cal. intake", fontsize=font_size
                        )
                    elif pos_var == 2:
                        ax.set_ylabel(
                            "Daily animal-based cal. intake", fontsize=font_size
                        )
                    elif pos_var == 3:
                        ax.set_ylabel("Share of animal-based cal.", fontsize=font_size)
                del (sub, pos_scenario)
        plt.tight_layout(w_pad=0.5)
        plt.savefig(
            path_data_output / f"felix3_reg_diet_sensi_{target_var}_all_region.svg",
            dpi=1200,
            # transparent=True,
            bbox_inches="tight",
            pad_inches=0.01,
            format="svg",
        )
exit()

# logging.info("Target variable is calorie intakes")
# target_var = target_vars[1]
# for pos_food, food_category in enumerate(food_categories):
#     df_var_food = df_all[
#         (df_all["variable"] == target_var) & (df_all["food_category"] == food_category)
#     ]
#     # --- Filter time range ---
#     df_var_food = df_var_food[
#         (df_var_food["time"] >= 100) & (df_var_food["time"] <= 200)
#     ]

#     # --- Extract simulation values (columns 2 onward) ---
#     value_cols = df_var_food.columns[
#         1:-5
#     ]  # exclude info, time, variable, region, food_category, scenario

#     # --- Compute statistics ---
#     stats = df_var_food.copy()
#     stats["median"] = df_var_food[value_cols].median(axis=1)
#     # stats["std"] = df[value_cols].std(axis=1)*100
#     stats["p25"] = df_var_food[value_cols].quantile(0.25, axis=1)
#     stats["p75"] = df_var_food[value_cols].quantile(0.75, axis=1)

#     fig, axes = plt.subplots(
#         nrows=1, ncols=4, figsize=(fig_width, fig_height), sharey=True
#     )
#     for pos_scenario, scenario_ in enumerate(scenarios):
#         ax = axes[pos_scenario]
#         for pos_region, region in enumerate(regions):
#             sub = stats[
#                 (stats["region"] == region) & (stats["scenario"] == scenario_)
#             ].sort_values("time")
#             ax.plot(sub["time"], sub["median"], label=region)
#             ax.fill_between(sub["time"], sub["p25"], sub["p75"], alpha=0.2)
#             ax.grid(False)

#             # ax.set_title(
#             #     scenario_mapping[scenario_], fontsize=font_size, fontweight="bold"
#             # )
#             ax.set_xlim(115, 200)
#             xticks = [115, 150, 200]
#             # Convert to years
#             xlabels = [t + 1900 for t in xticks]
#             # ax.set_xticks(xticks, xlabels, fontsize=font_size, rotation=90)
#             ax.set_xticks(xticks, [])

#             # if pos_food == 0:
#             #     ax.set_ylim(0, 100)
#             # elif pos_food == 1:
#             #     ax.set_ylim(0, 400)
#             # elif pos_food == 2:
#             #     ax.set_ylim(0, 720)
#             # elif pos_food == 3:
#             #     ax.set_ylim(0, 60)
#             # elif pos_food == 4:
#             #     ax.set_ylim(0, 360)
#             # elif pos_food == 5:
#             #     ax.set_ylim(0, 1800)
#             # elif pos_food == 6:
#             #     ax.set_ylim(0, 800)
#             # elif pos_food == 7:
#             #     ax.set_ylim(0, 1600)

#             # yticks = [
#             #     ytick_
#             #     for ytick_ in range(
#             #         int(ax.get_ylim()[0]),
#             #         int(ax.get_ylim()[1]) + 1,
#             #         int((ax.get_ylim()[1] - ax.get_ylim()[0]) / 4),
#             #     )
#             # ]
#             # ylabels = [f"{y_tick_}" for y_tick_ in yticks]
#             # ax.set_yticks(yticks, ylabels, fontsize=font_size)
#             ax.tick_params(axis="both", labelsize=7)
#             if pos_scenario == 0:
#                 ax.set_ylabel(f"Daily cal. intake, {food_category}", fontsize=font_size)

#     plt.tight_layout(w_pad=0.5)
#     # plt.show()
#     plt.savefig(
#         path_data_output
#         / f"felix3_reg_diet_sensi_{target_var}_{food_category}_all.svg",
#         dpi=1200,
#         # transparent=True,
#         bbox_inches="tight",
#         pad_inches=0.01,
#         format="svg",
#     )
#     plt.close()
# exit()
