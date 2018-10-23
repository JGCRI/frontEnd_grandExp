# A collection of functions for the grand experiment

library(foreach)
library(fldgen)   # (https://stash.pnnl.gov/users/snyd535/repos/fldgen/browse?at=refs%2Fheads%2Fstreamline_grandEXP) must be on the streamline_grandEXP branch
library(an2month) # (https://github.com/JGCRI/an2month/tree/streamline_grandEXP) must be on the streamline_grandEXP branch
library(reticulate)
library(dplyr)
library(tidyr)
library(lubridate)
library(doParallel)


# Initialize inputs and setings for Grand Experiment
#
# Import files and check inputs; set up python interpreter correctly for PIC.
#
# Returns:
#   List of length two containing mapping and tgavSCN data
init <- function(mapping, tgavSCN, xanthos_dir) {
  stopifnot(file.exists(mapping))
  stopifnot(file.exists(tgavSCN))
  stopifnot(dir.exists(xanthos_dir))

  mapping <- read.csv(mapping, header = TRUE, stringsAsFactors = FALSE)
  tgavSCN <- read.csv(tgavSCN, header = TRUE, stringsAsFactors = FALSE)

  check_columns(mapping, c("emulatorName", "trainingFile"), 'mapping input')
  check_columns(tgavSCN,  c('run_name', 'time', 'tgav'), 'tgavSCN input')
  stopifnot(is.character(mapping$trainingFile))
  stopifnot(file.exists(mapping$trainingFile))

  # Make sure that reticulate can find the version of python from `module load`
  Sys.setenv(RETICULATE_PYTHON = Sys.which('python'))
  stopifnot(py_numpy_available(initialize = TRUE))

  return(list(mapping = mapping, tgavSCN = tgavSCN))
}


# Use Hector to emulate ESM tas and pr monthly gridded data as inputs into Xanthos
#
# Args:
#   mapping: a path to the emulator mapping csv file containing a data frame of
#     emulatorName and trainingFile paths
#   tgavSCN: the path to the Hector temperature output stream file containing a
#     data frame that is used as the mean field when generating the tas and pr
#     full grids, must contain total temp in degree K.
#     NOTE – this will change to a list of hector ini files.
#   xanthos_dir: Full file path to xanthos input/output directory
#   N: the number of tas and pr realizations to produce for each emulator and
#     temperature scenario
#   globalAvg_file: an optional argument used to indicate the end of the annual
#     global average data to use during emulator training, if set to NULL then
#     the function will use the global average calculated internally by the
#     training function.
#   cores: an optional argument to specify the number of cores to parallelize
#     over, if set to NULL then uses default doParallel::registerDoParallel
#     settings.
grand_experiment <- function(mapping, tgavSCN, xanthos_dir, N, globalAvg_file = 'GlobalAvg.txt', cores = NULL) {
  inputs <- init(mapping, tgavSCN, xanthos_dir)

  # Apply over the emulators defined in the emulator mapping file and use the
  # training files to train the tas and pr emulator.
  lapply(split(inputs$mapping, inputs$mapping$emulatorName), function(emulator_mapping) {

    # TODO remove Ngrid when update the fldgen branch
    emulator <- fldgen::trainTP(dat = emulator_mapping[['trainingFile']], Ngrid = NULL, globalAvg_file = globalAvg_file)

    # Apply over the different Hector runs.
    # NOTE - this is going to change when we run Hector from here. When we do
    # this we will have to add a method to convert from Tgav C to tas K.
    lapply(split(inputs$tgavSCN, inputs$tgavSCN$run_name), function(tgav_input) {

      # Subset the Hector tas data so that it only includes values from the
      # emulator training years.
      # TODO this section might change with fldgen updates.
      start_stop <- as.integer(unlist(stringr::str_split(stringr::str_sub(basename(emulator$infiles), -12, -4), pattern = '-')))
      start_yr   <- min(start_stop)
      stop_yr    <- max(start_stop)
      tgav_input <- tgav_input[tgav_input$time %in% start_yr:stop_yr, ]

      # Generate N number of gridded realizations
      # TODO when we run on pic we are going to want to use
      # doParallel::registerDoParallel() and %dopar% to parallelize this step
      foreach::foreach(i = 1:N) %do% {

        # Generate the full tas and pr annual grids without any NA values.
        full_grids <- fldgen::generate.TP.fullgrids(emulator = emulator, residgrids = generate.TP.resids(emulator, 1),  tgav = tgav_input, addNAs = FALSE)

        # Replace the negative pr values in the full grids with 0s.
        # TODO This is just a temporary fix and will eventually be removed.
        negative_pr_cells <- which(full_grids$fullgrids[[1]]$pr < 0)
        full_grids$fullgrids[[1]]$pr[negative_pr_cells] <- 0

        # Monthly downscaling
        # TODO make the frac argument dynamic so that it reflects the emulator ESM.
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

        run_id <- paste(unique(emulator_mapping$emulatorName),
                        unique(tgav_input$run_name), i, sep = '_')
        run_xanthos(xanthos_dir, monthly_pr, monthly_tas, run_id)
      }
    })
  })
}


# Check a data frame for required columns
#
# Args:
#   df: a data frame to check
#   req_cols: a string vector of the required column names
#   dfName: an optional argument for the data frame name that will be
#     incorporated into the error message if a column is missing
# Returns:
#   if df is missing a req_cols then the function throws an error
check_columns <- function(df, req_cols, dfName = NULL) {

  missing <- !req_cols %in% names(df)

  if(any(missing)) stop(dfName, ' missing columns ', req_cols[missing])

}


# Run Xanthos
#
# Args:
#   xanthos_dir: Full file path to xanthos input/output directory
#   monthly_pr: List containing data (2d matrix), coordinates (lat/lon mapping),
#     and units (string)
#   monthly_tas: List containing data (2d matrix), coordinates (lat/lon mapping),
#     and units (string)
#   run_id: Unique identifier for each run, used to name Xanthos output directory
run_xanthos <- function(xanthos_dir, monthly_pr, monthly_tas, run_id) {
  stopifnot(monthly_pr$units == "mm_month-1")
  stopifnot(monthly_tas$units == "C")

  # Load reference file mapping Xanthos cell index to lat/lon
  xcells_path <- paste0(xanthos_dir, 'input/reference/coordinates.csv')
  xcolnames <- c('cell_id', 'lon', 'lat', 'lon_idx', 'lat_idx')
  xcells <- read.csv(xcells_path, header = F, col.names = xcolnames)

  ycolnames <- c('index', 'lat', 'lon')
  fldgencells <- monthly_pr$coordinates
  names(fldgencells) <- ycolnames

  pr_x  <- extract_xanthos_cells2d(monthly_pr$data, xcells, fldgencells)
  tas_x <- extract_xanthos_cells2d(monthly_tas$data, xcells, fldgencells)

  # Input temperature data for Xanthos as an R matrix. Must be a named list
  # where the name is the Xanthos config parameter it is replacing.
  xth_params <- list(trn_tas = tas_x, PrecipitationFile = pr_x,
                     OutputFolder = paste0(xanthos_dir, 'output/', run_id))

  # Instantiate and run Xanthos
  config_path <- paste0(xanthos_dir, 'trn_abcd_mrtm_gexp.ini')
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

  # Rows and columns need to be flipped
  cells[ , bothcells$index]
  return(t(cells))
}
