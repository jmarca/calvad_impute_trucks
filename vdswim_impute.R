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

vds_id = Sys.getenv(c('VDS_ID'))[1]
if('' == vds_id){
  print('assign a vds_id to process to the VDS_ID environment variable')
  stop(1)
}
vds_id <- as.numeric(vds_id)

year = as.numeric(Sys.getenv(c('RYEAR'))[1])
if('' == year){
  print('assign the year to process to the RYEAR environment variable')
  stop(1)
}

vds.root = Sys.getenv(c('CALVAD_VDS_PATH'))[1]
if('' == vds.root){
    if(! is.null(config.calvad.vdspath) && config.calvad.vdspath != '' ){
        vds.root <- config.calvad.vdspath
    }else{
        print('assign a path to find the imputed vds data to the CALVAD_VDS_PATH environment variable or set calvad: {vdspath : '/the/vds/path'} in the config file')
        stop(1)
    }
}

output.path <- Sys.getenv(c('CALVAD_OUTPUT_PATH'))[1]
if('' == output.path || is.null(output.path)){
    if(! is.null(config.calvad.outputpath) && config.calvad.outputpath != '' ){
        output.path <- config.calvad.outputpath
    }else{
        print(paste('CALVAD_OUTPUT_PATH environment variable is not set and no entry in config file under calvad:{outputpath:...}, so setting output path to',vds.root))
        output.path <- vds.root
    }
}

maxiter = Sys.getenv(c('CALVAD_VDSWIM_IMPUTE_MAXITER'))[1]
if(is.null(maxiter)){
    maxiter=200
}


## I don't think I use this anywhere
## wim.vds.pairs <- get.list.closest.wim.pairs()


impute.vds.site(vdsid=vds_id,
                wim_sites=wim_sites,
                year=year,
                vds_path=vds.root,
                output_path=output.path,
                maxiter=maxiter,
                trackingdb=config.couchdb.trackingdb
                )
