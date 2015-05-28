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

district = Sys.getenv(c('RDISTRICT'))[1]

if('' == district){
  print('assign a district to the RDISTRICT environment variable')
  exit(1)
}

file = Sys.getenv(c('FILE'))[1]
if('' == file){
  print('assign a file to process to the FILE environment variable')
  exit(1)
}

year = as.numeric(Sys.getenv(c('RYEAR'))[1])
if('' == year){
  print('assign the year to process to the RYEAR environment variable')
  exit(1)
}

district.path=paste(district,'/',sep='')

file.names <- strsplit(file,split="/")
file.names <- file.names[[1]]
fname <-  strsplit(file.names[length(file.names)],"\\.")[[1]][1]
vds.id <-  calvadrscripts::get.vdsid.from.filename(fname)
pems.root = Sys.getenv(c('CALVAD_PEMS_ROOT'))[1]
path = paste(pems.root,district,sep='')
file <- paste(path,file,sep='/')
print(file)
goodfactor <-   3.5
seconds = 120
## using maxiter must be a number, not string
## so the env var is irritating.  Hard code at 20 for now
maxiter = Sys.getenv(c('CALVAD_VDS_IMPUTE_MAXITER'))[1]
if('' == maxiter){
    maxiter=20
}
print(maxiter)

## fix everything below here
output.path <- Sys.getenv(c('CALVAD_IMPUTE_PATH'))[1]
if(is.null(output.path)){
  print('assign a output.path to the CALVAD_IMPUTE_PATH environment variable')
  exit(1)
}

localcouch = Sys.getenv(c('CALVAD_LOCAL_COUCH'))[1]
if(is.null(localcouch)){
  localcouch = FALSE
}else{
  localcouch = TRUE
}


vds.id = Sys.getenv(c('CALVAD_VDSID'))[1]
if(is.null(vds.id)){
  print('assign a file to process to the FILE environment variable')
  exit(1)
}

year = as.numeric(Sys.getenv(c('CALVAD_YEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  exit(1)
}

seconds <- 3600

maxiter = Sys.getenv(c('CALVAD_VDSWIM_IMPUTE_MAXITER'))[1]
if(is.null(maxiter)){
    maxiter=200
}

pems.root = Sys.getenv(c('CALVAD_PEMS_ROOT'))[1]
if(is.null(maxiter)){
    pems.root <- '/data/backup/pems'
}

## I don't think I use this anywhere
## wim.vds.pairs <- get.list.closest.wim.pairs()


impute.vds.site(vds.id,year,path=pems.root,maxiter=maxiter)
