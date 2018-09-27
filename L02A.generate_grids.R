
# Purpose: This script generates the temperature and percipitation grids from the fldgen trained emulator and the 
# results from Hector. Users should look at section 1 for user decisions about the emulator and the hector output 
# streams to use. 


# 0. Set Up ----------------------------------------------------------------------------------------------------------------------
showMessages <- TRUE # if set to FALSE in order to suppress messages
if(showMessages) message('0. Set Up ')

# Set up the directories
#BASE   <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp'
BASE <- getwd()
OUTPUT <- file.path(BASE, 'output-L2'); dir.create(OUTPUT, showWarnings = FALSE, recursive = TRUE)
dir.create(OUTPUT, showWarnings = FALSE, recursive = TRUE)

# Load the required libraries
library(tibble)
library(dplyr)
library(devtools)
library(tidyr)
load_all(file.path(BASE, 'fldgen')) # import the developmental package


# 1. User decisions ---------------------------------------------------------------------------------------------------------------

# Import the emulator inforamtion csv 
emulator_info <- read.csv(file.path(BASE, 'output-L1', 'emulator','emulator_info.csv'), stringsAsFactors = FALSE)

# TODO As of now there we are only working with a single emulator but we may want to modify this section of code 
# so that it can work with mulitple emulators and users can select different emulators.  
emulator_name <- unique(emulator_info$emulator)
emulator <- readRDS(file = list.files(file.path(BASE, 'output-L1', 'emulator'), emulator_name, full.names = TRUE))

# Import the hector tgav file to use when generating the temp and percip grids, keep on the years from 2006 to 2100 
# since we trained with the rcp experiment.
hector_tgav <- read.csv(file = file.path(BASE, 'output-L1', 'hector', 'hector_tgav.csv')) 
hector_tgav <- filter(hector_tgav, year %in% 2006:2099)


# 2. Generate grids --------------------------------------------------------------------------------------------------------------

# First generate a single residual grid
resid_grid <- generate.TP.resids(emulator, ngen = 1)


# Generate the grids for each of the hecotr run_names 
split(hector_tgav, hector_tgav$run_name) %>%  
  lapply(function(hector_input){
    
    # Format generate.TP.fullgrids input
    tgav <- select(hector_input, time = year, tgav)
    
    # Generate the full grids
    full_grids <- generate.TP.fullgrids(emulator, resid_grid, tgav)
    
    # Add some information to the output
    full_grids$run_name <- unique(hector_input$run_name)
    full_grids$emulator_trianing <- emulator$infiles
    
    # Return output
    full_grids
      
  }) -> 
  full_grids

# TODO there should be a better way to determine the ESM_name and a better pattern for the file names.
ESM_name <- 'ipsl-cm5a-lr' 

# Save the full grid output 
lapply(names(full_grids), function(grid_name){
  
  output <- file.path(OUTPUT, paste0(ESM_name, '_', grid_name, 'rds'))
  saveRDS(object = full_grids[[grid_name]], file = output)
  
})

# The end



