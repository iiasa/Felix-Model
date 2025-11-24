# -*- coding: utf-8 -*-
"""
Created: Mon 9 September 2024
Description: Scripts to download metadata of UNCOMTRADE via api
Scope: FeliX model regionalization
Author: Quanliang Ye
Institution: Radboud University
Email: quanliang.ye@ru.nl
"""
import datetime
import json
import logging
import os
from pathlib import Path
from time import time
from bs4 import BeautifulSoup

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

metadata_name = "reporter"
region_url = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"
region = requests.get(region_url).json()
region = pd.DataFrame(region["results"])
region.to_csv(f"scripts/comtrade/list_of_{metadata_name}.csv", index=False)
del metadata_name

metadata_name = "partner"
region_url = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
region = requests.get(region_url).json()
region = pd.DataFrame(region["results"])
region.to_csv(f"scripts/comtrade/list_of_{metadata_name}.csv", index=False)
