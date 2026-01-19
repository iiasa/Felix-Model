# FeliX3_ISE_v26 (by R. Tan)
- Based on FeliX3_YoGL_v25
### Food System Updates
- **Food Demand Scenarios**
  - Diet Change: Added alternative Global Scenario in *Diet Change Module*
  - Food Loss and Waste: Split into separate Waste/Losses components
  - Alternative Proteins: Newly scenario which include Land Swap interaction in *Land Use Module*
- **Food-related Indicators**
  - *Nutrition Module*: Newly added to calculate caloric and nutrition supply, including indicators such as PoU (Prevalence of Undernourishment)
  - Freshwater Withdrawal: Added formulas to estimate food-related water use
- **Additional Variables for FeliX ISE Tool**
  - Historical Data (New View)
  - Placeholder Values (New View)
### Climate System Updates
- **GHG Disaggregation**
  - *Emissions Module*: Newly added to track various emissions (previously only CO2) from various activities
  - *Gaseous Cycle Module*: Extended Carbon Cycle to include methane and nitrous oxide cycles
  - *Climate Module*: Updated formulas for radiative forcing that account for overlapping interactions between different GHG atmospheric concentrations
### Energy Updates
- **Wind/Solar Energy Modules**
  - Updated cost structure
  - Revised Energy Market Share Module

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