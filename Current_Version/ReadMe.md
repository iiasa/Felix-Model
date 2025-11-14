# FeliX Version 26
## Directory Guide
- **/Inputs_SSP_Scenarios**: SSP scenarios in VDFX/CSV formats (alignments: `Reference.csv`→SSP2, `Optimistic.csv`→SSP1, `Pessimistic.csv`→SSP3). It has been updated since v25.
  - These sets of parameters/constants can be loaded into FeliX (Simulation Control > Constant Changes) to experiment with various scenarios.
- **/Datasets**: Historical data (`Historical_v26.vdfx`), climate scenarios (`ssp126.vdfx`, `ssp245.vdfx`, `ssp370.vdfx`)  from the RCMIP protocol, and old model runs. 
  - Historical data is collected from a wide range of sources that was used for the calibration of FeliX . 
  - Climate scenarios dataset contains various emission and forcing trajectories under three SSP-RCP scenarios (SSP1-2.6, SSP2-4.5, SSP3-7.0) for reference.
- **/VPM_Files_ReadOnly**: Packaged model files for Vensim Model Reader (`FeliX3_ISE_v26.vpmx`). 
  - These are read-only versions that can be used with the Vensim Model Reader.

All VDFX dataset files can be placed in the same directory as the model file to be loaded and viewed in Vensim.

## Versions of FeliX
### V26
- New Food Demand Scenarios: Exogenous Diet Change and Alternative Proteins
- Food Indicators: Nutrition and Freshwater Consumption
- Climate System GHG Disaggregation
- Energy Cost and Market Share Updates for Solar and Wind 

These updates are documented in [Eker et al. (2025)](/Documentation/FeliX_merged_documentation.pdf) updated model documentation and the [online documentation](https://iiasa.github.io/felix_docs/1_1_global_version.html).

### V25
- Aggregate wellbeing measure: Years of Good Life
- Age- and gender-dependent poverty rates
- Age- and gender-dependent life expectancy and healthy life expectancy
- Climate impacts on economic output (GDP) and human mortality

These updates are documented in [Eker et al.(2023)](https://pure.iiasa.ac.at/id/eprint/18984/). This version is used in Chapter 5 of the [IIASA Flagship Report](https://iiasa.ac.at/sites/default/files/2023-09/IIASA%20Flagship%20Report.pdf) and Eker et al. (*under review*) *Wellbeing Cost of Carbon*.
