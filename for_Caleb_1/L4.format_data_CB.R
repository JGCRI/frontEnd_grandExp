
# Purpose: Format the data so that the data is ready to import into Xanthos. This is just a preliminary script, I think
# ultimately this will be included in the R sub dir as a package function.


# 0. Set Up ----------------------------------------------------------------------------------------------------

BASE <- "/pic/projects/GCAM/Dorheim/grand_exp/an2month"           # The project location.
#ISIMIP_DATA  <- "/pic/projects/GCAM/CMIP5-CHartin"               # Directory containing the cmip5 files to process.
INPUT_DIR  <- file.path(BASE, "data-raw", "output-L0", "isimip", "final")  # Define the output directory.
OUTPUT_DIR <- "/pic/projects/GCAM/Caleb/from_kalyn"

library(tibble)
library(dplyr)
library(ncdf4)
library(tidyr)

# Find the netcdf files to process
files <- list.files(INPUT_DIR, 'nc', full.names = TRUE)

# 1. Format Output ----------------------------------------------------------------------------------------------------

# TODO - remove the subset step this is just to meet CB's requirements

# format_output
# Is a function that formats the data from a netcdf into an array of matrices that will with lat, lon, and time
# information. This function requires input_path (the path to the nc file with the data to format),
# the output_path (the directory location where to save the output to), and showMessages a T/F argument that
# if set to F supresses function messages. Output is saved as .rds files and the location of the new file
# is returned by the function.
format_output <- function(input_path, output_path, showMessages = FALSE){

  # TODO Pull out file information based on input path, right now it is set up to work with the isip speicifc
  # file name strucutre but I would like to change it.
  tibble(file = basename(input_path)) %>%
    separate(col = file, into = c("A", "variable", "B", "C", "D", "model", "experiment", "E"), sep = '_') ->
    data_info

  # Pull out the variable name from the data info, this will be used to extract the data for the
  # variable from the netcdf file.
  vari <- data_info$variable

  # Open the nc file
  nc <- nc_open(input_path)

  # Extract data
  data <- ncvar_get(nc, vari)
  lat   <- as.vector(ncvar_get(nc, 'lat'))
  lon   <- as.vector(ncvar_get(nc, 'lon'))

  # Save unit information
  units <- ncatt_get(nc, vari, 'units')$value

  # Extract and format time
  time <- as.vector(ncvar_get(nc, 'time'))
  time <- paste0(substr(x = time, start = 1, stop = 6), "") # remove the date values
  styr  <- min(time)
  endyr <- max(time)

  # Add lon, lat, and time information to the data
  dimnames(data) <- list(lon, lat, time)

  # Subset the data so that it only include values from 1971 until 2001
  data <- data[ , , as.integer(substr(x = dimnames(data)[[3]], start = 1, stop = 4)) %in% 1971:2001 ]

  # Save data
  file_name <- paste0(paste(vari, data_info$model, units, styr, endyr, sep = '_'), '.rds')

  if(showMessages) message('saving ', file.path(OUTPUT_DIR, file_name))
  saveRDS(object = data, file = file.path(OUTPUT_DIR, file_name))

  if(showMessages) message('done saving rds')

  file.path(OUTPUT_DIR, file_name)

}


# lapply the format_output to all of the functions to process.
files <- lapply(files, FUN = format_output, output_path = OUTPUT_DIR, showMessages = TRUE)




