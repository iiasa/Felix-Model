## CHANGELOG for the regionalized version of FeliX

## Version 13.0.0
- Based on version 12.0.0
- Add water module, regionalized

## Version 12.0.0
- Based on version 11.0.0
- Add climate damage global module.

## Version 11.0.0
- Based on version 10.0.0
- Add climate global module.

## Version 10.0.0
- Based on version 9.6.8
- Add carbon cycle global module.


## Version 9.6.8
- Based on version 9.6.7
- Solar and wind demand, in EastEu region, is modeled as a share in total energy demand according to market share.

## Version 9.6.7
- Based on version 9.6.6
- Solar and wind demand, in LAC region, is modeled as a share in total energy demand according to market share.


## Version 9.6.6
- Based on version 9.6.5
- Solar and wind demand, in African region, is modeled as a share in total energy demand according to market share.

## Version 9.6.5
- Based on version 9.6.4
- Solar and wind demand, in WestEu region, is modeled as a share in total energy demand according to market share.

## Version 9.6.4
- Based on version 9.5.4
- Solar and wind demand, in AsiaPacific region, is modeled as a share in total energy demand according to market share. 

## Version 9.5.4
- Based on version 9.4.4
- Coal and Biomass denmand local is as a share in total energy demand according to market share


## Version 9.4.4
- Based on version 9.3.4
- Gas denmand local is as a share in total energy demand according to market share

## Version 9.3.4
- Based on version 9.2.4
- Total energy demand as a function of gdp per capita
- Oil denmand local is as a share in total energy demand according to market share

## Version 9.2.4
- Based on version 9.1.4
- Energy demand as a function of gdp per capita

## Version 9.1.4
- Based on version 9.0.4
- Market module has been calibrated based on shares of different energy sources in total supply
- next step is to implement demand-market-production mechanism

## Version 9.0.4
- Based on version 8.3.4
- Add Market share module
- Test the module

## Version 8.3.4
- Based on version 8.2.4
- Modify the fossil fuel energy modules, to make sure regional data follow the associated data in the global version
- 

## Version 8.2.4
- Based on version 8.1.4
- Correct net-import roles for regions, i.e., when one certain region is regarded as net importer, no export exists for this region; Roles of net import depend on their historic demand and production data.
- adjust initial values of some stock variables

## Version 8.1.4
- Based on version 8.0.4
- Update biomass production module according to global average prices of biomass energy

## Version 8.0.4
- Based on version 7.0.4
- Add wind energy module 
- solar energy prices per kWh are obtained from IRENA
- 

## Version 7.0.4
- Based on version 6.0.4
- Add solar energy module 
- solar energy prices per kWh are obtained from IRENA
- 

## Version 6.0.4
- Based on version 5.4.4
- Add biomass module 
- Historic biomass energy production from IEA, using domestic production data

## Version 5.4.4
- Based on version 5.3.4
- Recalibrate parameters in coal module to match historic trends of coal production
- Add import matrix of coal, and feed into export demand from the exporters, in this case, coal demand include both local demand and export demand 
- 

## Version 5.3.4
- Based on version 5.2.4
- Recalibrate parameters in nature gas module to match historic trends of gas production
- Add import matrix of gas, and feed into export demand from the exporters, in this case, gas demand include both local demand and export demand 
- 

## Version 5.2.4
- Based on version 5.1.4
- Population and GDP in LAC

## Version 5.1.4
- Based on version 5.0.4
- using modeled ratio between demand and production

## Version 5.0.4
- Based on version 4.0.4
- Change variables' name including "Fossil Fuel" into "coal"
- Add Energy Coal module, and solve exising errors
- Add the regional dimension
- Recalibrate parameters in oil module to match historic trends of oil production
- Change the oil price based on global demand / global production
- Add import matrix of oil, and feed into export demand from the exporters, in this case, oil demand include both local demand and export demand 


## Version 4.0.4
- Based on version 3.0.4
- Change variables' name including "Fossil Fuel" into "Gas"
- Add Energy Gas module, and solve exising errors
- Add the regional dimension

## Version 3.0.4
- Based on version 3.0.0
- Add the regional dimension
- Change variables' names including "Fossil Fuel" into "Oil"

## Version 3.0.0
- Based on version 2.2.4
- Add Energy Oil module, and solve exising errors
- NO REGIONS INCLUDED

## Version 2.2.4
- Based on version 2.2.3
- Further disaggregate the labor force elasticity into age cohorts (two general cohorts, young and old)
- Calabriate all relevant parameters


## Version 2.2.3
- Based on version 2.1.3
- Using simulated GWP to calculate GWP per capita in the model
- Remove the data variable of population data by YQL


## Version 2.1.3
- Based on version 2.0.3
- Define the labor force elasticity, which differs among labor force type (skill and unskill)
- Calabriate parameters to calculate labor force elasticity


## Version 2.0.3
- Based on version 1.5.3
- Change varibales about labor force, and use actual labor force participation rates (from International Labor Organization) to calculate labor force input.
- Add economy module for regionalization
- Calculate the capital intensity per output (capital stock per GWP) to estimate initial capital in 1900, data source from Groningen Growth and Development Centre
- Calibrate parameter values to estimate GWP 
- Add biodiversity module for regionalization, which contains Mean Species Abundance (MSA)


## Version 2.0.2 -- not implemented yet
- Based on version 1.5.2
- Add economy module for regionalization


## Version 2.0.1 -- not implemented yet
- Based on version 1.5.1
- Add economy module for regionalization


## Version 2.0.0 -- not implemented yet
- Based on version 1.5.0
- NOT USABLE ANYMORE!!!
- Add economy module for regionalization


## Version 1.5.3
- Based on version 1.4.3
- Using simulated population to calculate GWP per capita in the model
- Remove the data variable of population data by YQL
- Recalibrate relevant parameters such as L0, L, k, x0 for GDP on fertility


## Version 1.5.2 -- not implemented yet
- Based on version 1.4.2
- Using simulated population to calculate GWP per capita in the model
- Remove the data variable of population data by YQL


## Version 1.5.1
- Based on version 1.4.1
- Using simulated population to calculate GWP per capita in the model
- Remove the data variable of population data by YQL


## Version 1.5.0
- Based on version 1.4.0
- NOT USABLE ANYMORE!!!
- Using simulated population to calculate GWP per capita in the model
- Remove the data variable of population data by YQL


## Version 1.4.3
- Based on version 1.3.3
- Using simulated MYS in the model
- Remove the data variable of MYS YQL
- Recalibrate relevant parameters such as L0, L, k, x0 for education on fertility


## Version 1.4.2
- Based on version 1.3.2
- Using simulated MYS in the model
- Remove the data variable of MYS YQL
- Recalibrate relevant parameters such as L0, L, k, x0 for education on fertility


## Version 1.4.1
- Based on version 1.3.1
- Using simulated MYS in the model
- Remove the data variable of MYS YQL
- Recalibrate relevant parameters such as L0, L, k, x0 for education on fertility
- Adjust look-up function of educational durations to fit MYS


## Version 1.4.0
- Based on version 1.3.0
- NOT USABLE ANYMORE!!!
- Using simulated MYS in the model
- Remove the data variable of MYS YQL
- Recalibrate relevant parameters such as L0, L, k, x0 for education on fertility


## Version 1.3.3

- Distinguish reference tertiary education enrollment fraction Init by three age cohorts
- Recalibrate graduates by different educational levels
- Adjust look-up function of educational durations to fit MYS


## Version 1.3.2

- Delete Persistence parimary
- Change the mechanism of tertiary education to the same mechanism of primary/secondary education
- Recalibrate graduates by different educational levels
- Adjust look-up function of educational durations to fit MYS


## Version 1.3.1

- Change Persistence parimary
- Recalibrate graduates by different educational levels


## Version 1.3.0

- Change the average duration of secondary education
- Modify the lookup function of education duration


## Version 1.2.1

- Change the function of age-specific fertility rates according to document "Estimating age-specific fertility rate in the World Population Prospects: A Bayesian modelling approach"


## Version 1.2.0

- Recalibrate parameters for key varibales like age-specific fertility rates, and graduates of different educational levels


## Version 1.1.1

- Add data-type variables such population, GWP, etc for early-stage development


## Version 1.0.0

- Copy-paste the population module
- Add a subscript "Regions" in the model
- Modify the subscripts and functions of relevant variables


## CHANGELOG for the Global version of FeliX

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
