## need node_modules directories
dot_is <- getwd()
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(node_paths, winslash = "/", mustWork = FALSE)
lib_paths <- .libPaths()
.libPaths(c(path, lib_paths))

## need env for test file
config_file <- Sys.getenv('R_CONFIG')

if(config_file ==  ''){
    config_file <- 'config.json'
}
print(paste ('using config file =',config_file))
config <- rcouchutils::get.config(config_file)

## pass it the raw data details, and either the raw data will get
## loaded and parsed and saved as a dataframe, or else the existing
## dataframe will get loaded.  In either case, the plots will get made
## and saved to couchdb


library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)

vds_id = Sys.getenv(c('CALVAD_VDS_ID'))[1]
if('' == vds_id){
  print('assign a vds_id to process to the CALVAD_VDS_ID environment variable')
  stop(1)
}
vds_id <- as.numeric(vds_id)

year = as.numeric(Sys.getenv(c('CALVAD_YEAR'))[1])
if('' == year){
  print('assign the year to process to the CALVAD_YEAR environment variable')
  stop(1)
}

vds_path = Sys.getenv(c('CALVAD_VDS_PATH'))[1]
if('' == vds_path){
    if(! is.null(config.calvad.vdspath) && config.calvad.vdspath != '' ){
        vds_path <- config.calvad.vdspath
    }else{
        print('assign a path to find the imputed vds data to the CALVAD_VDS_PATH environment variable or set calvad: {vdspath : '/the/vds/path'} in the config file')
        stop(1)
    }
}

output_path <- Sys.getenv(c('CALVAD_OUTPUT_PATH'))[1]
if('' == output_path || is.null(output_path)){
    if(! is.null(config.calvad.outputpath) && config.calvad.outputpath != '' ){
        output_path <- config.calvad.outputpath
    }else{
        print(paste('CALVAD_OUTPUT_PATH environment variable is not set and no entry in config file under calvad:{outputpath:...}, so setting output path to',vds_path))
        output_path <- vds_path
    }
}

maxiter = Sys.getenv(c('CALVAD_VDSWIM_IMPUTE_MAXITER'))[1]
if(is.null(maxiter)){
    maxiter=200
}

wim_pairs = Sys.getenv(c('CALVAD_WIM_PAIRS'))[1]
if(is.null(wim_pairs) || '' == wim_pairs){
    print(paste('assign the wim_pairs for site',vds_id,'to the CALVAD_WIM_PAIRS environment variable',collapse=' '))
    stop(1)
}
## need to convert the CSV string of wim_pairs info into a proper R list


print(paste('calling impute with options',
            paste(c(vds_id,
                    wim_pairs,
                    year,
                    vds_path,
                    output_path,
                    maxiter,
                    config.couchdb.trackingdb)
                    ,collapse=' ',sep=',')
                  ))

## debugging
stop(1)

result <- impute.vds.site(vds_id=vds_id,
                          wim_pairs=wim_pairs,
                          year=year,
                          vds_path=vds_path,
                          output_path=output_path,
                          maxiter=maxiter,
                          trackingdb=config.couchdb.trackingdb
                          )

print(paste('imputation output saved to',result))
quit(save='no',status=10)
