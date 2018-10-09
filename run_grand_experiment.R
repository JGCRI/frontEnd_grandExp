# Purpose: Create the monthly gridded isimip IPSL data for the grand experiment.

# 0. Set Up --------------------------------------------------------------------------------------------------------------------

BASE       <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp' # the project location on pic
OUTPUT_DIR <- '/pic/projects/GCAM/Caleb/from_kalyn'

library(dplyr)
library(lubridate)
library(devtools)
devtools::load_all(file.path(BASE, 'an2month')) # these repos must be set to the streamline_grandEXP branches as of 10/8/2018
devtools::load_all(file.path(BASE, 'fldgen'))

# Import the emulator mapping file that will be used to train the temp precip emulator.
emulator_mapping <- read.csv(file.path(BASE, 'input', 'emulator_mapping.csv'))

# Import the Hector temperature data that defines the scenarios to emulate.
temp_scenarios <- read.csv(file.path(BASE, 'input', 'hector_tgav.csv'))


# 1. Functions  ----------------------------------------------------------------------------------------------------------------
frontEnd_grandExp <- function(mapping, N, tgavSCN, globalAvg_file = NULL, outputDir){
  
  # Check the emulator mapping data frame
  req_cols <- c("emulatorName", "trainningFile")
  missing  <- !req_cols %in% names(mapping)
  if(any(missing)){ stop('mapping input is missing ', paste(req_cols[missing], collapse = TRUE)) }
  
  # Check the tgav scenario data frame 
  req_cols <- c('run_name', 'time', 'tgav') 
  missing <- !req_cols %in% names(tgavSCN)
  if(any(missing)){ stop('tgavSCN input is missing ', paste(req_cols[missing], collapse = TRUE)) }
  
  # Check output directory 
  stopifnot(dir.exists(outputDir))
  
  message('done checking inputs')
  
  # For all of the emulators in the mapping file 
  # TODO remove the list setp
  lapply(split(mapping, mapping$emulatorName), function(df){
    
    message('starting to train emulator')
    emulator <- fldgen::trainTP(dat = df[['trainningFile']], Ngrid = NULL, globalAvg_file = globalAvg_file) 
    message('emulator trainning complete')
    
    # For all of the temperature scenarios in the temperature scenario data frame
    lapply(split(tgavSCN, tgavSCN$run_name)[1], function(tgav_input){
      
      # Save a copy of the run name 
      run_name <- unique(tgav_input$run_name)
      message(run_name)
      
      # Generate the full tas and pr annual grids without any NA values.
      full_grids <- generate.TP.fullgrids(emulator = emulator, 
                                          residgrids = generate.TP.resids(emulator, N), 
                                          tgav = tgav_input, 
                                          addNAs = FALSE)
      message('generate residual grids')
      
      # TODO make this dynamic, frac / number of runs / esm ect... 
      # For each of the full grids generated, downscale to monthly and save output! there is porbably 
      # something better to do with the coordinates file... 
      run_id <- sprintf('%05d', 1:N)
      
      lapply(1:length(run_id), function(index){
        message('monthly downscaling')
        # Monthly down scaling! 
        monthly_pr <- an2month::monthly_downscaling(var = 'pr', frac = an2month::`ipsl-cm5a-lr_frac`, fld = full_grids$fullgrids[[index]],
                                                    fld_coordinates = full_grids$coordinates, fld_time = full_grids$time)
        monthly_tas <- an2month::monthly_downscaling(var = 'tas', frac = an2month::`ipsl-cm5a-lr_frac`, fld = full_grids$fullgrids[[index]],
                                                     fld_coordinates = full_grids$coordinates, fld_time = full_grids$time)
        
        message('saving results for ', run_id[index])
        out_time <- paste0(names(monthly_pr$data)[1], '-', names(monthly_pr$data)[length(monthly_pr$data)])
        
        out_pr  <- file.path(outputDir, paste0('pr_', run_name, "_ipsl-cm5a-lr_", monthly_pr$units, '_', out_time, '_', run_id[index], '.rds'))
        out_tas <- file.path(outputDir, paste0('tas_', run_name, "_ipsl-cm5a-lr_", monthly_pr$units, '_', out_time, '_', run_id[index], '.rds'))
        
        saveRDS(monthly_pr, file = out_pr)
        saveRDS(monthly_tas, file = out_tas)
        
        # TODO! remove this? 
        write.csv(monthly_pr$coordinates, file = file.path(outputDir, "index_lat_lon.csv"), row.names = FALSE)
        
        # Return a vector of where the files were saved
        c(out_pr, out_tas)
        
      }) 
      
    })
    
    
  })
  
} 

# 2. Run the grand experiment --------------------------------------------------------------------------------------------------
time <- system.time(rslt <- frontEnd_grandExp(mapping = emulator_mapping, N = 5, 
                                              tgavSCN = temp_scenarios, globalAvg_file = 'GlobalAvg.txt', outputDir = OUTPUT_DIR))

message(time)


