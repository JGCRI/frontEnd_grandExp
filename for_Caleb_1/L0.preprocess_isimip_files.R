
# Purpose: this script finds the daily ISIMIP on pic, then averages from daily to monthly values. Right now this
# script is not going to be fully fleshed out because we are trying to get an example output to CB ASAP. Okay
# so this is fairly messy, but I figured out that the daily data is too big to concatenate together so
# first average to monthly to go to bleh. (this script was devloped pretty quickly for CB)


# 0. Set Up --------------------------------------------------------------------------

library(dplyr)
library(tidyr)
library(tibble)
library(purrr)

BASE         <- "/pic/projects/GCAM/Dorheim/grand_exp/an2month"     # The project location.
#ISIMIP_DATA  <- "/pic/projects/GCAM/CMIP5-CHartin"                 # Directory containing the cmip5 files to process.
OUTPUT_DIR   <- file.path(BASE, "data-raw", "output-L0", "isimip")  # Define the output directory.
CDO_DIR      <- "/share/apps/netcdf/4.3.2/gcc/4.4.7/bin/cdo"        # Define the cdo directory.

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# 1. Find ISIMIP data -------------------------------------------------------------------

# Add code that finds the ISIMIP files that match some serach pattern. But since we are just
# trying to get the example output to CB pick the specific pr and tas files to work with.
#
# nc_paths <- c('/pic/projects/GCAM/leng569/data/Input_GCM_bced/IPSL-CM5A-LR/rcp2p6/pr_bced_1960_1999_ipsl-cm5a-lr_rcp2p6_2006-2010.nc4',
#               '/pic/projects/GCAM/leng569/data/Input_GCM_bced/IPSL-CM5A-LR/rcp2p6/tas_bced_1960_1999_ipsl-cm5a-lr_rcp2p6_2006-2010.nc4')


# Okay because CB wants data from 1971 to 2001 we are going to have to concatenate the historcal stuff togther
pr_paths  <- list.files('/pic/projects/GCAM/leng569/data/Input_GCM_bced/IPSL-CM5A-LR/historical', 'pr_bced', full.names = TRUE)[2:16]
tas_paths <- list.files('/pic/projects/GCAM/leng569/data/Input_GCM_bced/IPSL-CM5A-LR/historical', 'tas_bced', full.names = TRUE)[2:16]


# There is going to be some code here that pulls out the meta inforamtion from the netcdf file.

# 2. Check coverage  -------------------------------------------------------------------

# There needs to be some code here that checks to make sure that the files that are going
# to be processed includes all of the files.

# 3. Process to monthly  -------------------------------------------------------------------

# Okay it takes too long to concatenate the daily nc files together so we are going to have
# to process all of the monthly and then go and concatnate, this is some what messy at the
# moment.

# monthly_func
# Is the function that calculates the monthly values from the daily netcdf files. This
# function requires the a path to a daily nc file, the path to the cdo, and a directory
# location where to save the netcdfs. There is an option showMessages T/F argument
# that can be used to supress function messages. The monthly netcdf will be saved at
# output_dir and the function will return the path/new.nc

# TODO remove the concatenate function! Just need this because CB wants the years that cover 1971 to 2001

monthly_func <- function(input_nc, cdo_dir, output_dir, showMessages = FALSE){

  # This function depends on CDO (https://code.zmaw.de/projects/cdo) being installed
  stopifnot(file.exists(cdo_dir))
  stopifnot(dir.exists(output_dir))

  base_name <- paste0('monthly_', basename(input_nc))
  out_nc    <- file.path(output_dir, base_name)

  # Get the mean and the convert to absolute time, when we start working on the full version
  # of this script we are going to need to update this section so that the converting to absolut
  # time happens at the L1 raw data processing script!
  if(showMessages) message("Calculate the monthly average ", input_nc)
  system2(cdo_dir, args = c("monmean", input_nc, out_nc), stdout = TRUE, stderr = TRUE)

  out_nc

}

concat_func <- function(files, cdo_dir, output_dir, showMessages = FALSE){

  # This function depends on CDO (https://code.zmaw.de/projects/cdo) being installed
  stopifnot(file.exists(cdo_dir))
  stopifnot(dir.exists(output_dir))

  base_name <- paste0(basename(files[1]))
  out_nc    <- file.path(output_dir, base_name)

  if(file.exists(out_nc)){
    if(showMessages) message("Removing old copy of ", out_nc)
    file.remove(out_nc)
  }


  if(showMessages) message("Concatenating files and converting to absolute time ", out_nc)
  system2(CDO_DIR, args = c("-a", "cat", files, out_nc), stdout = TRUE, stderr = TRUE)

  out_nc

}

# lapply the cdo monthly_func to all of the daily netcdf files to process.
#output_ncs <- lapply(ncfiles_paths, FUN = monthly_func, cdo_dir = CDO_DIR, output_dir = OUTPUT_DIR, showMessages = TRUE)

# Create an intermediate directory to save the monthly averages in
inter_dir <- file.path(OUTPUT_DIR, 'inter'); dir.create(inter_dir)

pr_ncs  <- unlist(lapply(pr_paths, FUN = monthly_func, cdo_dir = CDO_DIR, output_dir = inter_dir, showMessages = TRUE))
tas_ncs <- unlist(lapply(tas_paths, FUN = monthly_func, cdo_dir = CDO_DIR, output_dir = inter_dir, showMessages = TRUE))


final_out <- file.path(OUTPUT_DIR, 'final'); dir.create(final_out)
pr_final  <- concat_func(pr_ncs, cdo_dir = CDO_DIR, output_dir = final_out, showMessages = TRUE)
tas_final <- concat_func(tas_ncs, cdo_dir = CDO_DIR, output_dir = final_out, showMessages = FALSE)


# 4. Save nc information  -------------------------------------------------------------------

# Create a tibble to save as a csv file that contains the paths and some additional information
# about the nc files to pass on to level 1 to calculate the monthly fraction data.

to_process <- tibble(file = c(pr_final, tas_final))
write.csv(x = to_process, file = file.path(OUTPUT_DIR, 'to_process.csv'), row.names = FALSE)


