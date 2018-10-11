# KALYN I THINK THAT YOU ARE GOING TO WANT TO CHANGE THIS TO JUST A FUNCTION.R file


# Purpose: this script defines the functions used to run the front end of the grand experiment, there
# are several TODOs that mark potential enhancements. This code depends on
# fldgen (https://stash.pnnl.gov/users/snyd535/repos/fldgen/browse?at=refs%2Fheads%2Fstreamline_grandEXP)
# and then an2month (https://github.com/JGCRI/an2month/tree/streamline_grandEXP)

library(foreach)
library(fldgen)
library(an2month)
library(reticulate)
library(dplyr)
library(tidyr)
library(lubridate)

# A function that will throw an error if the data frame is missing columns. Optional dfName argument
# adds information to the error message.
check_columns <- function(df, req_cols, dfName = NULL){

  missing <- !req_cols %in% names(df)

  if(any(missing)) stop(dfName, ' missing columns ', req_cols[missing])

}

# TODO remove the Sys.time calls!!
# mapping:
#   Data from emulator_mapping.csv
# N:
#   Number of iterations
# tgavSCN:
#   Temperature scenario, data from hector_tgav.csv
#
# mapping <- read.csv('/Users/brau074/Documents/grand_experiment/emulator_mapping.csv', header=T, stringsAsFactors = F)
# tgavSCN <- read.csv('/Users/brau074/Documents/grand_experiment/hector_tgav.csv', header = T, stringsAsFactors = F)
grand_experiment <- function(mapping, N, tgavSCN, globalAvg_file = 'GlobalAvg.txt'){

  x1 <- Sys.time()

  # Check inputs
  check_columns(mapping, c("emulatorName", "trainningFile"), 'mapping input')
  check_columns(tgavSCN,  c('run_name', 'time', 'tgav'), 'tgavSCN input')
  stopifnot(is.character(mapping$trainningFile))
  stopifnot(file.exists(mapping$trainningFile))

  x2 <- Sys.time() - x1
  message('Done checking inputs: ', x2)

  lapply(split(mapping, mapping$emulatorName), function(emulator_mapping){

    x3 <- Sys.time()
    # TODO I think that the Ngrid argument will be dropped evenutally due to pacakges changes
    # Train the emulator with the trainning files specified in the mapping file, the emulator
    # will be used to generate the residuals and the temp/pr full grids.
    emulator <- fldgen::trainTP(dat = emulator_mapping[['trainningFile']], Ngrid = NULL, globalAvg_file = globalAvg_file)
    x4 <- Sys.time() - x3
    message('Done trainning emulator: ', x4)

    # TODO this will eventually be replaced with some mechanism to run hector with different ini files!
    # TODO this is going to need to be chagned so that it looks at year not time!
    lapply(split(tgavSCN, tgavSCN$run_name), function(tgav_input){

      # TODO we need to figure out what years to subset the hector output for so that it matches the
      # emulator trainning years. Right now now the best way to extract years is from the training name,
      # hopefully there will be a better method with the user enhancements to fldgen.
      start_stop <- as.integer(unlist(stringr::str_split(stringr::str_sub(basename(emulator$infiles), -12, -4), pattern = '-')))
      start_yr   <- min(start_stop)
      stop_yr    <- max(start_stop)

      tgav_input <- tgav_input[tgav_input$time %in% start_yr:stop_yr, ]


      # TODO we might want to change this to %dopar% to parallelize this process
      foreach(i = 1:N) %do% {

        x5 <- Sys.time()
        # Generate the full tas and pr annual grids without any NA values.
        full_grids <- fldgen::generate.TP.fullgrids(emulator = emulator, residgrids = generate.TP.resids(emulator, 1),  tgav = tgav_input, addNAs = FALSE)
        x6 <- Sys.time() - x5
        message('Done generating full grids for itteration ', i, ' ', x6)


        x7 <- Sys.time()
        # Downscale to monthly time step!
        # TODO there needs to be some mechanism so that the fraction used to downscale is not hard coded... i wonder if we want to add some esm column
        # to the emulator mapping file and then update the fraction file names in an2month...
        monthly_pr  <- an2month::monthly_downscaling(
          var = 'pr',
          frac = an2month::`ipsl-cm5a-lr_frac`,
          fld = full_grids$fullgrids[[1]],
          fld_coordinates = full_grids$coordinates,
          fld_time = full_grids$time
        )
        monthly_tas <- an2month::monthly_downscaling(
          var = 'tas',
          frac = an2month::`ipsl-cm5a-lr_frac`,
          fld = full_grids$fullgrids[[1]], fld_coordinates = full_grids$coordinates,
          fld_time = full_grids$time
        )
        x8 <- Sys.time() - x7
        message('Down temporal downscaling for itteration ', i, ' ', x8)

        # add the code to run xanthos here
        run_xanthos(monthly_pr, monthly_tas)

      }

    })

  })

}


# Run Xanthos
#
# monthly_pr:
#   List containing data (2d matrix), coordinates (lat/lon mapping), and units (string)
# monthly_tas:
#   List containing data (2d matrix), coordinates (lat/lon mapping), and units (string)
run_xanthos <- function(monthly_pr, monthly_tas) {

  # Full file path to xanthos directory
  XANTHOS_DIR <- '/Users/brau074/Documents/xanthos-work/xanthos/'

  # Load reference file mapping Xanthos cell index to lat/lon
  xcells_path <- paste0(XANTHOS_DIR, 'example/input/reference/coordinates.csv')
  xcolnames <- c('cell_id', 'lon', 'lat', 'lon_idx', 'lat_idx')
  xcells <- read.csv(xcells_path, header = F, col.names = xcolnames)

  ycolnames <- c('index', 'lat', 'lon')
  fldgencells <- monthly_pr$coordinates
  names(fldgencells) <- ycolnames

  pr_x  <- extract_xanthos_cells2d(monthly_pr$data, xcells, fldgencells)
  tas_x <- extract_xanthos_cells2d(monthly_tas$data, xcells, fldgencells)


  # Input temperature data for Xanthos as an R matrix. Must be a named list where
  # the name is the Xanthos config parameter it is replacing.
  xth_params <- list(pm_tas = tas_x, PrecipitationFile = pr_x)
  xth_params <- list(PrecipitationFile = pr_x)


  # Instantiate and run Xanthos ---------------------------------------------

  # Instantiate Xanthos
  config_path <- paste0(XANTHOS_DIR, '../trn_abcd_mrtm.ini')
  xth.mod <- import('xanthos')
  xth <- xth.mod$Xanthos(config_path)

  # Run Xanthos, passing in the parameter list
  xth$execute(xth_params)
}


# Retrieve Xanthos grid cells from alternately indexed vectors
#
# Convert spatial data from a list of numeric vectors (each list element is one
# month) to the input format expected by Xanthos (land cell x months).
#
# Args:
#   cells: A list of numeric vectors
#   xcells: Xanthos index to lat/lon map
#   ycells: Map for the input vectors' indices to lat/lon
#
# Returns:
#   2d array of Xanthos cells by month
extract_xanthos_cells2d <- function(cells, xcells, ycells) {
  stopifnot(all(c('cell_id', 'lat', 'lon') %in% names(xcells)))
  stopifnot(all(c('index', 'lat', 'lon') %in% names(ycells)))
  stopifnot(nrow(ycells) == nrow(xcells))

  bothcells <- ycells %>%
    inner_join(xcells, by = c('lat', 'lon')) %>%
    arrange(cell_id) # Order by Xanthos cell id for indexing

  cells[ , bothcells$index]
  return(t(cells))
}


