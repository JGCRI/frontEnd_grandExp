
# Purpose: this script does the monthly down scaling and formats the data for CB, this requires a lot of procesing power, will 
# want to have an empty environment before running


# TODO: this should be more felxible but we are working on a time constraint. 

# TODO: add the L04B.fromating stuff to this script! 

# 0. Set up ----------------------------------------------------------------------------

library(dplyr)
library(lubridate)


#BASE <- getwd()
#OUTPUT <- file.path(BASE, 'output-L3'); dir.create(OUTPUT)
BASE <- '/pic/projects/GCAM/Dorheim/frontEnd_grandExp'
OUTPUT <- '/pic/projects/GCAM/Caleb/from_kalyn'


devtools::load_all(file.path(BASE, 'an2month'))

# TODO this should come from the files being processed
model <- "ipsl-cm5a-lr"


# TODO there should be some way to serach for these instead of manually doing it
# Do the monthly downsacing for pr 
# pr rcp 45 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp45rds'), 
                    var = "pr") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('pr_rcp4p5_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)

# tas rcp 45 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp45rds'), 
                    var = "tas") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('tas_rcp4p5_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)


# pr rcp 60 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp60rds'), 
                    var = "pr") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('pr_rcp6p0_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)


# tas rcp 60 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp60rds'), 
                    var = "tas") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('tas_rcp6p0_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)
# pr rcp 85 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp85rds'), 
                    var = "pr") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('pr_rcp8p5_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)

# tas rcp 85 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp85rds'), 
                    var = "tas") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('tas_rcp8p5_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)

# pr rcp 26 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp26rds'), 
                    var = "pr") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('pr_rcp2p6_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))


# clean up 
remove(output)

# tas rcp 26 --------------------------------------------------------------
monthly_downscaling(frac_path = file.path(BASE, 'an2month', 'data', 'ipsl-cm5a-lr_frac.rda'), 
                    flds_path = file.path(BASE, 'output-L2', 'ipsl-cm5a-lr_hector-grandExp_rcp26rds'), 
                    var = "tas") -> 
  output

startYr <- names(output$data)[1]
endYr <- names(output$data)[2]

file_name <- paste0('tas_rcp2p6_', model, '_', output$units, "_", startYr, "_", endYr, '.rds')

saveRDS(output$data, file = file.path(OUTPUT, file_name))






# Save a copy of the coordinates --------------------------------------------------------

write.csv(output$coordinates, file = file.path(OUTPUT, 'index_lat_lon.csv'), row.names = FALSE) 