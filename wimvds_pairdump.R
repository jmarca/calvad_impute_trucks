## This is a top-level R script
##
## it will dump wim-vds pair data set to CSV

## need node_modules directories
dot_is <- getwd()
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(node_paths, winslash = "/", mustWork = FALSE)
lib_paths <- .libPaths()
.libPaths(c(path, lib_paths))

pkg <- devtools::as.package('.')
ns_env <- devtools::load_all(pkg,quiet = TRUE)$env

## need env for test file
config_file <- Sys.getenv('R_CONFIG')

if(config_file ==  ''){
    config_file <- 'config.json'
}
print(paste ('using config file =',config_file))
config <- rcouchutils::get.config(config_file)


## pass it the details on what to do via environment variables

wim_site <- Sys.getenv(c('CALVAD_WIM_SITE'))[1]
if('' == wim_site){
  print('assign a WIM site number to the CALVAD_WIM_SITE environment variable')
  stop(1)
}
wim_site <- as.numeric(wim_site)

wim_dir <- Sys.getenv(c('CALVAD_WIM_DIR'))[1]
if('' == wim_dir){
  print('assign a WIM direction (N,S,E, or W) to the CALVAD_WIM_DIR environment variable')
  stop(1)
}

year <- Sys.getenv(c('CALVAD_YEAR'))[1]
if('' == year){
  print('assign the year to process to the CALVAD_YEAR environment variable')
  stop(1)
}
year <- as.numeric(year)

output_path <- Sys.getenv(c('CALVAD_OUTPUT_PATH'))[1]
if('' == output_path || is.null(output_path) ||  is.na(output_path)){
    if(! is.null(config$calvad$outputpath) && config$calvad$outputpath != '' ){
        output_path <- config$calvad$outputpath
    }
}
if('' == output_path || is.null(output_path) ||  is.na(output_path)){
    print(paste('CALVAD_OUTPUT_PATH environment variable is not set and no entry in config file under calvad:{outputpath:...}.  Please set a suitable output path'))
    stop(1)
}

print(paste('calling dump.wim.csv with options',
            paste(c(wim_site,wim_dir,year,
                       wim_path=path,
                       output_path=output_path,
                    trackingdb=config$couchdb$trackingdb
                    ))))

result <- dump.wim.csv(wim_site,wim_dir,year,
                       wim_path=path,
                       output_path=output_path,
                       trackingdb = config$couchdb$trackingdb)

print(paste('paired and merged file dumped as CSV to',result))
quit(save='no',status=10)
