# -*- coding: utf-8 -*-
"""
Created: Fri 23 Feb 2024
Description: Scripts to calibrate parameters used in logistic funcation of mortality fractions
Scope: FeliX model regionalization---calibration
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""

import datetime
import json
import logging
import math
import re
from pathlib import Path
import os
import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyam
import seaborn as sns
import yaml
from matplotlib import cm
from matplotlib.collections import PolyCollection
from matplotlib.colors import Normalize
from matplotlib.gridspec import GridSpec
from mpl_toolkits.axes_grid1 import make_axes_locatable
from scipy import stats
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error, r2_score

timestamp = datetime.datetime.now()
file_timestamp = timestamp.ctime()

# predefine the name of variable
variable = "mortality_fraction"  # need to hard coding
# read config.yaml file
yaml_dir = Path("scripts/calibration/config.yaml")
with open(yaml_dir, "r") as dimension_file:
    data_info = yaml.safe_load(dimension_file)

version = data_info["version"]
felix_module = data_info["module"]
path_data_file = Path(data_info["data_path"]).joinpath(data_info["data_file"])
data_sheet = data_info["data_sheet"]

# Any path consists of at least a root path, a version path, a module path
path_clean_data_folder = Path(data_info["data_output"]["root_path"]).joinpath(
    f"version_{version}/{felix_module}"
)

# set logger
if not (path_clean_data_folder.joinpath("logs")).exists():
    (path_clean_data_folder.joinpath("logs")).mkdir(
        parents=True,
    )
logging.basicConfig(
    level=logging.DEBUG,
    filename=f"{path_clean_data_folder}/logs/{timestamp.strftime('%d-%m-%Y')}.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# add the handler to the root logger
logging.getLogger().addHandler(console)


logging.info("Extracting information of input data")
input_data_info = {}
for dataset in data_info["data_input"]:
    if dataset["variable"] == variable:
        input_data_info = dataset
if not input_data_info:
    logging.warning(
        f"No {variable} data set up in the {felix_module} configed in .yaml"
    )
    raise KeyError
logging.info("Information of input data is loaded")

logging.info("Extracting dimension information for data cleaning and restructing")
regions = data_info["dimension"]["region"]
genders = data_info["dimension"]["gender"]
age_cohorts = data_info["dimension"]["age"]
logging.info("Extracted dimensions of regions, genders, and ages")

# Read input data
logging.info(f"Read input data")
input_calibration = pd.read_excel(
    path_data_file,
    sheet_name=data_sheet,
).rename(columns={"Time": "parameter"})


def create_data_files(
    input_data_all: pd.DataFrame,
    input_data_info: dict,
    output_data_path: Path,
    **kwargs,
):
    """
    To create structured data for calibration

    Parameter
    ---------
    input_data_all: pd.DataFrame
        The input data file that inlcudes all parameters between 1900 and 2100

    input_data_info: dict,

    **kwargs
        Other arguments that may be used to clean input data

    Returns
    -------
    a list of dependent variables
    """
    logging.info("Set information of the data need to be calibrated")
    # Any path consists of at least a root path, a version path, a module path
    cal_data = input_data_info["variable"]
    cal_data_text = input_data_info["text"]

    logging.info("Set the dependency data for calibration")
    dep_var_all = []
    for dep_var_info_ in input_data_info["dependency_variable"]:
        dep_var = dep_var_info_["dep_variable"]  # need hard coding
        locals()[f"dep_text_{dep_var}"] = dep_var_info_["dep_text"]
        dep_var_all.append(dep_var)
        del dep_var

    for region in regions:
        logging.info(f"Extracting calibration data for region {region}")
        cal_param = f"{cal_data_text}[{region}"
        cal_data_input = input_data_all[
            input_data_all["parameter"].apply(lambda x: cal_param in x)
        ]
        cal_data_input = cal_data_input.dropna(
            axis=1,
            how="all",
        )
        years_ava = cal_data_input.columns.to_list()[1:]

        logging.info(f"Extracting dependent calibration data for region {region}")
        cal_data_input_dep = pd.DataFrame()
        for var in dep_var_all:
            var_text = locals()[f"dep_text_{var}"]
            cal_param_dep = f"{var_text}[{region}"
            cal_data_input_dep_ = input_data_all[
                input_data_all["parameter"].apply(lambda x: cal_param_dep in x)
            ][["parameter"] + years_ava]
            cal_data_input_dep = pd.concat(
                [
                    cal_data_input_dep,
                    cal_data_input_dep_,
                ]
            )
            del var, var_text, cal_param_dep, cal_data_input_dep_

        logging.info(f"Write the required data for region {region} for calibration")
        file_name = f"calibration_{cal_data}_{region}.csv"
        pd.concat([cal_data_input, cal_data_input_dep]).to_csv(
            output_data_path.joinpath(file_name),
            index=False,
        )
    return dep_var_all


def logistic_functions(
    x: float,
    L0: float,
    L: float,
    k: float,
    x0: float,
):
    """
    Define different logistic equations


    Parameter
    ---------
    x: float
        The value of dependency variable

    x_ref: float
        The reference value of dependency variable, mostly using the value in 2000

    L0: float
        Parameter used in the logistic function

    L: float
        Parameter used in the logistic function

    k: float
        Parameter used in the logistic function

    x0: float
        Parameter used in the logistic function

    Returns
    -------
    The estimated value of the calbration variable
    """
    if variable == "mortality_fraction":
        exp_term = np.exp(-k * (x / cal_data_dep_ref[cal_data_dep_all[0]][region] - x0))
        return L0 + L / (1 + exp_term)


def parameter_calibrating(
    cal_data_input: pd.DataFrame,
    cal_params: list,
    **kwargs,
):
    """
    To calibrate parameters used in a logistic equation


    Parameter
    ---------
    cal_data_input: pd.DataFrame
        Created historic data of the variable

    cal_params: list,
        A list of parameters that need to be calibrated

    **kwargs
        Other arguments that may be used for calibration

    Returns
    -------
    Calibrated parameters in pd.Dataframe
    """
    logging.info("Set the number of variables for calibration")
    cal_data_num = len(cal_data_input) - 1
    cal_data = cal_data_input.iloc[:-1, :]
    # df_par = pd.DataFrame(index=df_mor['Age'], columns=['L0','L', 'k', 'x0', 'r2']).dropna()
    cal_data_dep = cal_data_input.iloc[-1, 1:].values.tolist()

    # runing time depends on the iteration time; currently set as 50000 times
    logging.info("Set iteration times, which will influnce the runing time")
    iteration_time = 50000
    logging.info(f"Set iteration times as {iteration_time}")
    cal_results = pd.DataFrame(
        index=cal_data_input["parameter"][:-1],
        columns=cal_params + ["r2"],
    )
    for i in range(cal_data_num):
        cal_data_hist = cal_data.iloc[i, 1:].values.tolist()
        try:
            n_par, n_cov = curve_fit(
                logistic_functions,
                cal_data_dep,
                cal_data_hist,
                maxfev=iteration_time,
                bounds=(
                    [
                        -np.inf,
                        -np.inf,
                        -np.inf,
                        -np.inf,
                    ],
                    [
                        np.inf,
                        np.inf,
                        0,
                        np.inf,
                    ],
                ),
            )
        except RuntimeError as e:
            logging.error(RuntimeError)

        cal_results.loc[cal_data["parameter"][i], ["L0", "L", "k", "x0"]] = n_par
        cal_data_est = logistic_functions(np.array(cal_data_dep), *n_par)
        cal_results.loc[cal_data["parameter"][i], "r2"] = r2_score(
            cal_data_hist,
            cal_data_est,
        )
        del cal_data_est, cal_data_hist

    logging.info(f"Finish calibratiing {variable} for {region}")
    return cal_results


# Start the calibration
logging.info(f"Start calibrating the {variable}")
logging.info(f"Create data files for calibration")
cal_data_dep_all = create_data_files(
    input_data_all=input_calibration,
    input_data_info=input_data_info,
    output_data_path=path_clean_data_folder,
)
logging.info(f"Created all data files for calibration")

logging.info(f"Call the calibration function")
cal_results_all = pd.DataFrame()
logging.info(f"Predefine the reference values for the dependency variable")

global cal_data_dep_ref
cal_data_dep_ref = {
    cal_data_dep_all[0]: input_data_info["dependency_variable"][0]["dep_ref"],
}

global region
for region in regions:
    logging.info(f"Read the input data for calibration")
    path_cal_data_file = path_clean_data_folder.joinpath(
        f"calibration_{variable}_{region}.csv"
    )
    cal_data_input = pd.read_csv(
        path_cal_data_file,
        encoding="utf-8",
    )
    logging.info(f"The input data {variable} in {region} will be used for calibration")

    logging.info(f"Set the calibration parameters")
    cal_params = input_data_info["parameter"]
    logging.info(f"The calibration parameters are {cal_params}")

    logging.info(f"Start calling the calibration function")
    cal_results = parameter_calibrating(
        cal_data_input=cal_data_input,
        cal_params=cal_params,
    )

    cal_results_all = pd.concat(
        [
            cal_results_all,
            cal_results,
        ]
    )
    os.remove(path_cal_data_file)
    del cal_results, cal_params, cal_data_input, path_cal_data_file, region

logging.info(f"Finish calibratiing {variable} for all regions")

logging.info(f"Write the calibration results")
cal_result_file_name = f"calibration_result_{variable}.csv"
cal_results_all.to_csv(
    path_clean_data_folder.joinpath(cal_result_file_name),
)
logging.info(f"The calibration results have been stored")
