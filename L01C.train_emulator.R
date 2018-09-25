
# Purpose: this script trains the flgden emulator using the specific netcdf files. 

# 0. Set Up -----------------------------------------------------------------------------------------------------------------------

BASE   <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp'
OUTPUT <- file.path(BASE, 'output-L1', 'emulator'); dir.create(OUTPUT, showWarnings = FALSE, recursive = TRUE)
dir.create(OUTPUT, showWarnings = FALSE, recursive = TRUE)

library(tibble)
library(dplyr)
library(devtools)
library(tidyr)
load_all(file.path(BASE, 'fldgen')) # import the developmental package

# Import the list of the nc files to use in training. 
to_process <- read.csv("/pic/projects/GCAM/Dorheim/frontEnd_grandExp/output-L1/annual_isimip/to_process.csv", stringsAsFactors = FALSE)

showMessages <- TRUE # if set to FALSE in order to suppress messages


# 1. Train Emulator -----------------------------------------------------------------------------------------------------------------
if(showMessages) message('1. Train Emulator')

# Use all of the rcp experiments in the to_process csv file to train the emulator
emulator_time <- system.time( emulator <- trainTP(dat = to_process$path, Ngrid = 259200,
                                                  tvarname = "tas", tlatvar = "lat", tlonvar = "lon",
                                                  pvarname = "pr", platvar = "lat", plonvar = "lon", globalAvg_file = 'GlobalAvg.txt'))

# Create a tibble to save some information about the emulator.
tibble(emulator = 'emulator-1',
       'number of files' = length(to_process),
       'training files' = to_process$path,
       'elapsed time' = emulator_time[['elapsed']]) ->
  emulator_info


# 2. Save emulator and info ----------------------------------------------------------------------------------------------------------
if(showMessages) message('2. Save emulator and info')

emulator_outputF <- file.path(OUTPUT, 'emulator-1.rds')
info_outputF     <- file.path(OUTPUT, 'emulator_info.csv')

saveRDS(emulator, file = emulator_outputF)
write.csv(emulator_info, file = info_outputF, row.names = FALSE)

if(showMessages) message('saving ', info_outputF, '\n', emulator_outputF)
if(showMessages) message('end of script')



