This model version includes the implementation of 

- a population clustering with age cohort, gender, and education level

- psychosocial dynamics of dietary shifts between meat-eating and vegetarianism

- food production system (demand, land use, crop yields, waste, production) divided into 8 food categories:
    - Pasture-based meat (beef and lamb)
    - Dairy
    - Poultry and pork
    - Eggs
    - Grains
    - Legumes
    - Fruits, vegetables and roots
    - Other crops (sugar and oil crops, nuts)
    
 - agricultural use of N and P fertilizers for each food category
 
The model file **FeliX3_DietChange_Eker_2019_NatSus_NoExcel.mdl** has no external connection, therefore can be simulated stand-alone.

The model file **FeliX3_DietChange_Eker_2019_NatSus.mdl** includes external connections to the *InitialValues.xls*, *New_DietData.xls*, *GLOBIOM.vdf* and *HistorcialData.vdf* to take inputs. Therefore, these data files should be in the same directory as the model file while running.
