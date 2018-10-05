# Purpose: remove the NAs from the output to make the files easier to work with this step needs to be included into the L4 script...

INPUT  <- '/pic/projects/GCAM/Caleb/from_kalyn/old'
OUTPUT <- '/pic/projects/GCAM/Caleb/from_kalyn'

paths <- list.files(INPUT, '.rds', full.names = TRUE)




lapply(paths, function(input){

  data <- readRDS(input)


  keep <- which(!is.na(data[[1]]))

  # Subset each of the matrices so that it does not inlcude the NAs
  lapply(data, function(df){

    df[keep]

  }) ->
    data_no_NA

  # save the output
  file <- file.path(OUTPUT, basename(input))
  saveRDS(data_no_NA, file)

})

# now we need to updat the lat and lon coordinates mapping file!

# Import the mapping file
mapping_path <-  list.files(INPUT, '.csv', full.names = TRUE)
mapping_file <- read.csv(mapping_path)
data <- readRDS('/pic/projects/GCAM/Caleb/from_kalyn/old/pr_rcp2p6_ipsl-cm5a-lr_mm_month-1_200601_209912.rds')

keep <- which(!is.na(data[[1]]))

mapping_no_NA <- mapping_file[keep, ]

file <- file.path(OUTPUT, basename(mapping_path))
write.csv(mapping_no_NA, file = file)




