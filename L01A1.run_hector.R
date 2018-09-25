
# Purpose: this script runs Hector set up on pic as part of the grand experiment. Script run time depends on the number 
# of Hector runs. This script is based off of LY's code from hector-SA-npar. The script is currently set up to run 4 hector 
# runs (very small supper quick, estimated running time less than a min)

# 0. Set Up ------------------------------------------------------------------------------------------------------------------

library(tidyr)

# Define the hector and frontEnd_grandExp location on pic. 
pic_hector_dir   <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp/hector'
pic_frontEnd_dir <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp'

# Define and if need be create the output directory. 
output_dir <- file.path(pic_frontEnd_dir, 'output-L1', 'hector'); dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Define the ini files and run names. The ini files must live in pic_hector_dir/input
ini_file <- c('hector_rcp26.ini', 'hector_rcp45.ini', 'hector_rcp60.ini', 'hector_rcp85.ini') 
run_name <- c('hector-grandExp_rcp26', 'hector-grandExp_rcp45', 'hector-grandExp_rcp60', 'hector-grandExp_rcp85')



# 1. Run Hector --------------------------------------------------------------------------------------------------------------
# Format the ini files and run names into a data frame, use the data frame to run hector multiple times. 
ini_df <- data.frame(ini_file = ini_file, run_name = run_name, stringsAsFactors = FALSE)

apply(X = ini_df, MARGIN = 1, FUN = function(input = X){
  
  # Read in the ini file so that minor change to the hector set up can be made, here we want to rename 
  # the outputstream to reflect the rcp used in hector.
  ini_path <- list.files(path = file.path(pic_hector_dir, 'input'), pattern = input[['ini_file']], full.names = TRUE )
  ini_file <- file(ini_path)
  temp_ini <- ini_template 
  
  # Rename the hector run.
  temp_ini[ 4 ] <- paste0( "run_name=", input[['run_name']] )
  writeLines( temp_ini, ini_file )
  close( ini_file )
  
  # Run hector
  setwd( pic_hector_dir )
  system( 'pwd' )
  exp <- paste0( './source/hector ', file.path('.', 'input', input[['ini_file']]))
  system( exp )
  
  # Copy the hector output stream to the pic_frontEnd_dir
  hector_output_path <- list.files(path = file.path(pic_hector_dir, 'output'), pattern = input[['run_name']], full.names = TRUE)
  output_file        <- file.path(output_dir, basename(hector_output_path))
  file.copy( hector_output_path, output_file )
  
  # Clean up
  file.remove(hector_output_path)
  
})

# End

