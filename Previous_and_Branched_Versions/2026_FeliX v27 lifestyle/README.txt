FeliX Version 27

Updates since v26: 

* Lifestyle module/view - represents lifestyle change through four lifestyle archetypes, with resulting behavioural changes translated through an Avoid-Shift-Improve structure into energy (residential and passenger transport) and food demand impacts.

* Disaggregation of energy demand into residential, commercial, transport (passenger and freight), and industry sector demands. 

Directory Guide:

* /Inputs_SSP_Scenarios: SSP-like scenarios in VDFX formats, with CSV of corresponding input parameters (SSP alignments: Reference_v27.csv/SSP2, Optimistic_27.csv/SSP1, Pessimistic_v27.csv/SSP3). It has been updated since v26.
o The sets of parameters/constants in the csv files can be further edited and loaded into FeliX (Simulation Control > Constant Changes) to experiment with various scenarios.
o Corresponding SSP vdfx files for scenario runs of lifestyle adjusted energy and food demand pathways (Reference_post_lifestyle_change_v27.vdfx, Optimistic_post_lifestyle_change_v27.vdfx, Pessimistic_post_lifestyle_change_v27.vdfx)
* /Datasets: Historical data (Data.vdfx, Historical_v27.csv) 
o Historical data is collected from a wide range of sources that was used for the calibration of FeliX; 
o Data.vdfx is the imported model data file, and the Historical_v27.csv contains the raw data and additional information.


This version runs on Vensim DSS. All VDFX files can be placed in the same directory as the model file to be loaded and viewed in Vensim.
