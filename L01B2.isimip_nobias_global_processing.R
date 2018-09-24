
# Purpose: Because the bias corrected ISIMIP netcdfs only includes values over land and fldgen 
# requires a global mean average this script processes the no bias corrected monthly 
# output to a annual global average. 

# Users should check that all the directories defined in section 0 exists and should check 
# out section 1 for decisions about what isimip files to search for. 

# TODO may be we want to include some code that will check to see if we have all the global 
# averages we need to train the fldgen emulator? Might have to update the script so that 
# the output includes a column name and a years column 

# 0. Set Up -----------------------------------------------------------------------------

showMessages <- TRUE # a T/F to suppress or show script messages
if(showMessages) message('0. Set Up')

library(dplyr)
library(tidyr)
library(tibble)
library(purrr)


BASE         <- "/pic/projects/GCAM/Dorheim/frontEnd_grandExp"   # The project location on pic
CDO_DIR      <- "/share/apps/netcdf/4.3.2/gcc/4.4.7/bin/cdo"     # Define the cdo directory
ISIMIP_DIR   <- "/pic/projects/GCAM/leng569/CMIP5"               # The location of the isimip files on pic
INTER_DIR    <- '/pic/scratch/dorh012'                           # The intermediate dir to write things out to 
OUTPUT_DIR   <- file.path(BASE, "output-L1", "annual_isimip"); dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# 1. User Decisions -----------------------------------------------------------------------------------------------------
# Select the ismip variables, experiment, model, and so on to process. The strings listed in each of the
# vectors will be used in a search pattern to identify the files to process from the ISIMIP_DIR. We are interested
# in the biased correct files and they have the following file pattern. If the vector is set to NULL then the
# pattern will search for all options.
# variable_bced_1960_1999_model_experiment_startYr-endYr.nc4
if(showMessages) message('1. User Decisions')


# isimip search vectors
VARIABLES       <- c("tas")                                   # isimip variables to process
EXPERIMENTS     <- c("rcp45", "rcp26", "rcp85", "rcp60")     # isimip experiments to process
MODELS          <- "IPSL-CM5A-LR"                             # isimip models to process
ENSEMBLES       <- "r1i1p1"                                   # the ensemble number


# 2. Find the isimip files to process -----------------------------------------------------------------------------------------------------
# Create the search netcdf search pattern and then search the isimip directory.
if(showMessages) message('2. Find the isimip files to process')

# pattern_gen is a function that converts the isimip search vectors from section
# into strings that will be used to create the regex search pattern. If the
# input vector is set to NULL then the function will return a search all pattern.
pattern_gen <- function(vector){
  
  if(is.null(vector)){
    "[a-zA-Z0-9-]+"
  } else {
    paste(vector, collapse = "|")
  }
  
}

varpattern        <- pattern_gen(VARIABLES)
modelpattern      <- pattern_gen(MODELS)
experimentpattern <- pattern_gen(EXPERIMENTS)
ensemblepattern   <- pattern_gen(ENSEMBLES)


# Create the pattern for the isimip file names for the netcdfs to search for.
isimip_search_pattern <- paste("(regridded)_(1deg)_(", 
                               varpattern,
                               ")_(Amon)_(", 
                               modelpattern, ")_(",
                               experimentpattern, ")_(",
                               ensemblepattern, ')_',
                               "([0-9]{6})-([0-9]{6}",          # time, set up to search for any 6 digit date
                               ").nc$", sep = "")
# Sanity check
if(showMessages) message("isimip file search pattern: ", isimip_search_pattern, "\n")


# Search for the files
file_list <- list.files(ISIMIP_DIR, pattern = isimip_search_pattern, full.names = TRUE, recursive = TRUE)

# Sanity check
if(length(file_list) < 1) stop('Could not find any isimip files files matching ', isimip_search_pattern)


# Parse out the run meta information from the file_list 
tibble(path = file_list) %>% 
  separate(path, into = c('A', 'B', 'variable', 'C', 'model', 'experiment', 'ensemble', 'yrs'), remove = FALSE, sep = '_' ) -> 
  to_process



# 4. Process to global annual average  -------------------------------------------------------------------
# Because we need a global annual average but only have access to monthly gridded nobias corrected data 
# we will use CDO to process to the correct data. 

if(showMessages) message('4. Process to global annual average')

# Create a mapping file to use to name the output so that it is compatible with the isismip 
# bias corrected files. 
tibble(experiment = c('rcp26', 'rcp45', 'rcp60', 'rcp85'), 
       new_experiment  = c('rcp2p6', 'rcp4p5', 'rcp6p0', 'rcp8p5')) %>%
  mutate(model = rep("IPSL-CM5A-LR", nrow(.)), 
         new_model = rep("ipsl-cm5a-lr", nrow(.))) -> 
  mapping_names


# The fldgen training function will want txt files with the format
# variable_annual_model_experiment_ensemble_start-end.ncglobalAvg.txt 
# So this function will use a mapping file to rename models / experiments to 
# match the isimip bias corrected nomenclature. 

annual_globalAvg_func <- function(df, cdo_dir, inter_dir, output_dir, mapping = mapping_names, 
                                  showMessages = FALSE, removeIntermediate = TRUE){
  
  # This function depends on CDO (https://code.zmaw.de/projects/cdo) being installed
  stopifnot(file.exists(cdo_dir))
  stopifnot(dir.exists(inter_dir))
  stopifnot(dir.exists(output_dir))
  
  # Based on the isimip file information create the output file name. 
  interOut1_nc <- file.path(inter_dir, paste0('inter-annual_avg-', basename(df[['path']])))
  interOut2_nc <- file.path(inter_dir, paste0('inter-global_avg-', basename(df[['path']])))
  
  
  # Get the global annual average. 
  # TODO in theory this could be accomplished by a single 
  if(showMessages) message("Convert to absolute time and annual average ", df[['path']])
  system2(cdo_dir, args = c("-a", "yearmean", df[['path']], interOut1_nc), stdout = TRUE, stderr = TRUE)
  
  if(showMessages) message("Calculate global average ", df[['path']])
  system2(cdo_dir, args = c("fldmean", interOut1_nc, interOut2_nc), stdout = TRUE, stderr = TRUE)
  
  
  # Extract the data from the global annual average nc to save as the txt file. 
  # TODO it looks like the isimip data only includes values to 2099, we will want to 
  # subset the results to match the number of years in the bias corrected data. 
  nc <- nc_open(interOut2_nc)
  nc_time <- ncvar_get(nc, 'time')
  nc_data <- ncvar_get(nc, df[['variable']])
  nc_units <- ncatt_get(nc, df[['variable']], 'units')[['value']]
  
  # The bias corrected data temp is in K so if we run into an issue where the 
  # tas data is in C convert it to K 
  if( df[['variable']] == 'tas' & nc_units == 'C'){
    
    if(showMessages) message('converting from deg C to K')
    # Convert from C to K 
    nc_data <- nc_data + 273.15
    
  }
  
  # TODO make this not hard coded
  # Subset the data so that it matches the time step of the output 
  # generated by L01B1. 
  start <- 2006 
  end   <- 2099 
  
  time <- as.integer(substr(nc_time, 1, 4))
  keep <- which(start <= time & time <= end)
  
  final_data <- nc_data[keep]
  
  # Now that we have the final data with the correct units we need to name 
  # the output file using the new names mapping file. 
  df_mapping <- left_join(x = df, y = mapping, by = c("model", "experiment")) 
     

  out_name <- paste0(df_mapping[['variable']], '_annual_', df_mapping[['new_model']], "_", 
                     df_mapping[['new_experiment']], '_xxx_', start, '-', end, '.ncGlobalAvg.txt')
  out_file <- file.path(output_dir, out_name)
  
  if(showMessages) message('saving ', out_file)
  write.table(final_data, file = out_file, row.names = FALSE, col.names = FALSE)
  
  
  if(removeIntermediate){
    
    file.remove(interOut1_nc, interOut2_nc)
    
  }
  
}


# Use lapply to apply the annual_globalAvg_func to process all of the files in the to process data frame. 
lapply(X = split(to_process, to_process$path), 
       FUN = annual_globalAvg_func, cdo_dir = CDO_DIR, inter_dir = INTER_DIR, output_dir = OUTPUT_DIR, mapping = mapping_names, 
       showMessages = showMessages, removeIntermediate = TRUE)

# Finish script 
if(showMessages) message('script complete')
