
# Purpose: the fldgen package used in L01B needs annual tas and pr data with a specific naming convention. The 
# only isimip data we have access to is daily and does not follow the correct naming convention. This script 
# averages from daily to annual values and formats the nc files to be used to train the fldgen pacakge. 
# This script uses a combination of R and cdo. Will need to submit a slurm job on pic. 

# Users should check that all the directories defined in section 0 exsit and should check out section 1 for 
# decisions about what isimip files to serach for. 

# 0. Set Up -----------------------------------------------------------------------------

showMessages <- TRUE # a T/F to supress or show script messages
if(showMessages) message('0. Set Up')

library(dplyr)
library(tidyr)
library(tibble)
library(purrr)


BASE         <- "/pic/projects/GCAM/Dorheim/frontEnd_grandExp"   # The project location on pic
CDO_DIR      <- "/share/apps/netcdf/4.3.2/gcc/4.4.7/bin/cdo"     # Define the cdo directory
ISIMIP_DIR   <- "/pic/projects/GCAM/leng569/data/Input_GCM_bced" # The location of the isimip files on pic
OUTPUT_DIR   <- file.path(BASE, "output-L1", "annual_isimip"); dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)


# 1. User Decisions -----------------------------------------------------------------------------------------------------
# Select the ismip varaiables, experiment, model, and so on to process. The strings listed in each of the
# vectors will be used in a serach pattern to identify the files to process from the ISIMIP_DIR. We are interested
# in the biased correct files and they have the following file pattern. If the vector is set to NULL then the
# pattern will serach for all options.
# variable_bced_1960_1999_model_experiment_startYr-endYr.nc4
if(showMessages) message('1. User Decisions')


# isimip search vectors
VARIABLES       <- c("tas", "pr")                             # isimip variables to process
EXPERIMENTS     <- c("rcp4p5", "rcp2p6", "rcp8p5", "rcp6p0")  # isimip experiments to process
MODELS          <- "ipsl-cm5a-lr"                             # isimip models to process


# 2. Find the isimip files to process -----------------------------------------------------------------------------------------------------
# Create the serach netcdf search pattern and then serach the isimip directory.
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


# Create the pattern for the isimip file names for the netcdfs to search for.
isimip_search_pattern <- paste("(", varpattern,
                               ")_(bced)_([0-9]{4})_([0-9]{4})_(", # the biased corrected portion of the pattern
                               modelpattern, ")_(",
                               experimentpattern, ")_",
                               "([0-9]{4})-([0-9]{4}",          # time, set up to search for any 4 digit date
                               ").nc4$", sep = "")
# Sanity check
if(showMessages) message("isimip file search pattern: ", isimip_search_pattern, "\n")


# Search for the files
file_list <- list.files(ISIMIP_DIR, pattern = isimip_search_pattern, full.names = TRUE, recursive = TRUE)

if(length(file_list) < 1) stop('Could not find any isimip files files matching ', isimip_search_pattern)


# Parse out the isimip meta data information from the file name, this will be use to make a table to
# help illustrate what we have and identify missing netcdf files.
tibble(path = file_list) %>%
  mutate(filename = basename(path)) %>%
  separate(filename, into = c("variable", "A", "B", "C", "model",
                              "experiment", "date"), sep = "_", remove = FALSE) %>%
  select(path, variable, model, experiment, date) %>%
  separate(date, into = c("startYr", "endYr"), sep = "-", remove = TRUE) %>%
  mutate(endYr = gsub(".nc4", "", endYr)) ->
  isimip_file_info


# 3. Check coverage  ----------------------------------------------------------------------
# We need to make sure that we have pr and tas files for each model / experiment / time period.
isimip_file_info %>%
  select(-path) %>%
  mutate(exsists = TRUE) %>%
  spread(variable, exsists) %>%
  gather(variable, exsist, -model, -experiment, -startYr, -endYr) ->
  check_variable

check_variable %>%
  filter(!exsist) %>%
  mutate(keep = FALSE) ->
  missing_variable

if(nrow(missing_variable)){
  
  # I am 99% sure that the files all match up, for now just throw an error message if it looks like
  # there might be incomplete netcdf coverage. May need to add code here latter.
  
  stop('Missing some variable need to add code to section 3 to
       remove incomplete netcdfs from further processing.')
  
}

# 4. Process to annual  -------------------------------------------------------------------
# Becasue we only have access to the daily values we are going to need to average to the
# annual value in order to train the fldgen emulator. Also the fldgen emulator assumes that 
# all of the data is concatnated into a single file, here we will have to concatenate the 
# annual results together! But this must happen in a seperate cdo call to avoid segementation 
# fault errors.

if(showMessages) message('4. Process to annaul')

# The fldgen training function only wants data that looks like 
# variable_annual_model_experiment_ensemble_start-end.nc 
# The isimip files do not have ensemble member numbers so just use xxxs inorder 
# to get the output nc file to match the expected pattern. 


annual_func <- function(df, cdo_dir, output_dir, showMessages = FALSE){
  
  # This function depends on CDO (https://code.zmaw.de/projects/cdo) being installed
  stopifnot(file.exists(cdo_dir))
  stopifnot(dir.exists(output_dir))
  
  # Based on the isimip file information create the output file name. 
  base_name <- paste0(df[['variable']], '_annual_', df[['model']], '_', df[['experiment']], '_xxx_',
                      df[['startYr']], '-', df[['endYr']], '.nc')
  out_nc    <- file.path(output_dir, base_name)
  
  # Get the annual average. 
  # TODO do we need to conver to absolute time here??? 
  if(showMessages) message("Calculate the annual average ", df[['path']])
  system2(cdo_dir, args = c("yearmean", df[['path']], out_nc), stdout = TRUE, stderr = TRUE)
  
  out_nc
  
}


concat_func <- function(df, cdo_dir, output_dir, showMessages = FALSE){

  # Input df must include information for a single model, variable, exerpiment and must
  # include a startYr and endYr columns.
  req_cols <- c('model', 'variable', 'experiment', 'startYr', 'endYr')
  missing  <- !req_cols %in% names(df)
  if(any(missing)) stop('input df is missing: ', paste(req_cols[missing], collapse = ', '))

  info <- distinct(select(df, model, variable, experiment))
  if(nrow(info) > 1) stop('problem with input df, trying to concatenate mulitple model / variable / experiments together')

  # This function depends on CDO (https://code.zmaw.de/projects/cdo) being installed
  stopifnot(file.exists(cdo_dir))
  stopifnot(dir.exists(output_dir))

  # Name the output nc
  startYr <- min(df$startYr)
  endYr   <- max(df$endYr)
  name    <- paste0(info[['variable']], '_annual_', info[['model']], '_', info[['experiment']], '_xxx_',
                    startYr, '-', endYr, '.nc')
  
  out_nc <- file.path(output_dir, name)
  file.remove(out_nc) # Remove the file to prevent a segmentation fault

  # Concatenate the annual data together and conver to absolute time
  if(showMessages) message("Concatenating files and converting to absolute time ", out_nc)
  system2(CDO_DIR, args = c("-a", "cat", df[['path']], out_nc), stdout = TRUE, stderr = TRUE)

  # Print the out_nc name
  out_nc

  
}


# Get the annual averages. 
annual_ncs <- apply(X = isimip_file_info, MARGIN = 1, FUN = annual_func, cdo_dir = CDO_DIR, output_dir = OUTPUT_DIR,
              showMessages = showMessages)


# Parse out the netcdf meta data from the file name in order to concatenate the files with the same variable, 
# model, and experiment together. 
tibble(path = ncs) %>%  
  mutate(file = gsub('.nc', '', basename(path))) %>% 
  separate(col = file, into = c('variable', 'A', 'model', 'experiment', 'B', 'time'), sep = '_') %>%  
  separate(col = time, into = c('startYr', 'endYr')) ->
  annual_ncs

to_concatenate <- split(annual_ncs, interaction(annual_ncs$variable, annual_ncs$model, annual_ncs$experiment))

final_ncs <- lapply(X = to_concatenate, FUN = concat_func, cdo_dir = CDO_DIR, output_dir = OUTPUT_DIR, showMessages = showMessages)


# 5. Clean up and save -------------------------------------------------------------------------------
# Delete the annual averages and then save a to process file of the netcdfs to use in the fldgen 
# emulator trainer.
if(showMessages) message('5. Clean up and save')

# Remove the intermediate annual nc files
file.remove(annual_ncs)

# Parse out the file information and save as a csv.
tibble(path = final_ncs) %>%  
  mutate(file = gsub('.nc', '', basename(path))) %>% 
  separate(col = file, into = c('variable', 'A', 'model', 'experiment', 'B', 'time'), sep = '_') -> 
  to_process 

write.csv(to_process, file = file.path(OUTPUT_DIR, 'to_process.csv'), row.names = FALSE)




