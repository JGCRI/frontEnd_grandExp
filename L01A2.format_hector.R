
# Purpose: Format the Hector output streams into the gloabl avaerge data frames that we wand to use to 
# generate the full temp and precip grids. Becasue Hector returns a temperature anomoly we are going to 
# have to add the preindustrial value to the output streams. 

# TODO: we will probably want to actually pull the values from the ESM global averages file but for now 
# they are jsut copied and pasted in from 
# /pic/projects/GCAM/Dorheim/frontEnd_grandExp/output-L1/annual_isimip/tas_annual_ipsl-cm5a-lr_rcp2p6_xxx_2006-2099.ncGlobalAvg.txt
# /pic/projects/GCAM/Dorheim/frontEnd_grandExp/output-L1/annual_isimip/tas_annual_ipsl-cm5a-lr_rcp4p5_xxx_2006-2099.ncGlobalAvg.txt
# /pic/projects/GCAM/Dorheim/frontEnd_grandExp/output-L1/annual_isimip/tas_annual_ipsl-cm5a-lr_rcp6p0_xxx_2006-2099.ncGlobalAvg.txt
# /pic/projects/GCAM/Dorheim/frontEnd_grandExp/output-L1/annual_isimip/tas_annual_ipsl-cm5a-lr_rcp8p5_xxx_2006-2099.ncGlobalAvg.txt
# Also need a way to do this for different ESMs we are trying to emulate. 
# Need to... 
# add more comments 
# spell check!


# 0. Set Up -------------------------------------------------------------------------------------------------------------------------------------------

library(tidyr)
library(dplyr)

# Define the hector and frontEnd_grandExp location on pic. 
BASE       <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp'
OUTPUT_DIR <- file.path(BASE, 'output-L1', 'hector'); dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# 1. Import and format data ----------------------------------------------------------------------------------------------------------------------------

list.files(path = OUTPUT_DIR, pattern = "outputstream_hector", full.names = TRUE) %>% 
  lapply(function(path){
    
    data <- read.csv(path, stringsAsFactors = FALSE, comment.char = '#')
    
  }) %>%  
  bind_rows -> 
  all_hector_output


# Subset for the Tgav variable durring non spin up years.
all_hector_output %>%  
  filter(variable == 'Tgav' & spinup == 0) -> 
  hector_Tgav

# Convert to absolute time 
# The 2006 temperature should be euqal for all of the rcps but use the 
# average hector 2006 temp value anyways. 
hector_Tgav %>% 
  filter(year == 2006) %>% 
  pull(value) %>%  
  mean -> 
  hector_Tgav_2006

# The average ISIMIP temp
# TODO see the note in the script header
ESM_temp_2006 <- mean(286.584524536133, 286.536165618896, 286.576834106445, 286.501993560791)

# Aproximate the preindustrial value
preindustrial <- ESM_temp_2006 - hector_Tgav_2006

# Add the preindustrial to hector_Tgav to convert temp anomoly to absolute temperature in K. 
hector_Tgav %>% 
  mutate(value = value + preindustrial) %>% 
  select(run_name, year, tgav = value) -> 
  hector_temp

# 2. Save Output ----------------------------------------------------------------------------------------------------------------------------
write.csv(hector_temp, file = file.path(OUTPUT_DIR, 'hector_tgav.csv'), row.names = FALSE)



