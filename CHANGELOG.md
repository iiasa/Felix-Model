# Changelog

## Version 3.0.4
- Based on version 3.0.0
- Add the regional dimension

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