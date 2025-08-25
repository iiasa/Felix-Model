import pandas as pd
import pyreadstat


file_name = "Modul_12B_Purchases_past6_months.sav"
df, meta = pyreadstat.read_sav(
    "C:/Users/yequanliang/OneDrive - IIASA/Desktop/LSMS 2012_eng/Data_LSMS 2012/"
    + file_name
)
print(df.head())
