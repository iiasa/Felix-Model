
# Ageing society (by Q. Ye)
- Simple modelling ageing in the regionalized felix v.15.3.0
- Data cleaning for CFPS
- Data cleaning for EuroSTAT. Note: data from EuroStat is in PPS (Purchasing power standard)
- Data cleaning for USA
- Data cleaning for Japan
- Data cleaning for AUS
- Merge data from different countries
- Convert currency to 2005 USD
- Specify age cohorts and expenditure items of FeliX
- Merge national expenditure to regional expenditure of Felix. MUST INCLUDE CHINA AS THE REPRESENTATIVE OF DEVELOPING COUNTRIES.
- Build a preliminary model
- Use Chinese survey data as a proxy, applied to upper middle income countries and adjusted according to household final consumption per capita data from World Bank
- Calibrating the expenditure module
- Regression analysis between expenditure and age, educational level, and income level.


# FeliX3_YoGL_v25 (by Q. Ye)
- Based on FeliX3_YoGL_v24
- Adjust the parameter values to match the total investment in three fossil fuels. Total investment inlcudes investment in oil exploration, oil production, and oil technology.
- Modify the function of "Effect of Oil Demand and Supply on Price", try to match the historic data of crude oil prices, however, cannot.
- Re-calibration parameters to fit the historic GWP data

# FeliX3_YoGL_v24 (by Q. Ye)
- Based on FeliX3_YoGL_v23
- Convert DELAY1, DELAY1I, and DELAY3 functions, including:
  - Graduation Rate from Tertiary Education
  - Graduation Rate from Primary Education
  - Graduation Rate from Secondary Education
  - Primary enrollment rate previous
  - Expected crop yield
  - Expected Grassland Milk Yield
  - Potential vegetarians
  - CCS Improvement Change
  - Increase in Ratio of Coal Fraction Recoverable to Unrecoverable
  - Increase in Ratio of Coal Fraction Discoverable to Undiscoverable
  - Increase in Wind Energy Technology Ratio
  - Increase in Wind Installation Technology Ratio
  - Expected Grassland Meat Yield
  - Forest Protected Land Change
  - Increase in Solar Installation Technology Ratio
  - Increase in Solar Energy Technology Ratio
  - Increase in Biomass Installation Technology Ratio
  - Increase in Biomass Energy Technology Ratio
  - Increase in Ratio of Gas Fraction Recoverable to Unrecoverable
  - Increase in Ratio of Gas Fraction Discoverable to Undiscoverable
  - Increase in Ratio of Oil Fraction Discoverable to Undiscoverable
  - Increase in Ratio of Oil Fraction Recoverable to Unrecoverable

# FeliX3_YoGL_v23 (by Q. Ye)
- Based on FeliX3_YoGL_v22
- Remove the exteral excel dependencies