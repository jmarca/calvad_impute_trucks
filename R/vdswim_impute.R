library('R.utils')
library('RPostgreSQL')
m <- dbDriver("PostgreSQL")

## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user=psqlenv[2]
                  ,password=psqlenv[3]
                  ,host=psqlenv[1]
                  ,dbname="spatialvds")


source('components/jmarca-rstats_couch_utils/couchUtils.R',chdir=TRUE)
source('components/jmarca-calvad_rscripts/lib/vds_impute.R',chdir=TRUE)

## source('components/jmarca-calvad_rscripts/lib/get.medianed.amelia.vds.R',chdir=TRUE)
## source('components/jmarca-calvad_rscripts/lib/amelia_plots_and_diagnostics.R',chdir=TRUE)
## source('components/jmarca-calvad_rscripts/lib/just.amelia.call.R',chdir=TRUE)
## source('components/jmarca-calvad_rscripts/lib/wim.loading.functions.R',chdir=TRUE)
## source("components/jmarca-calvad_rscripts/lib/vds.processing.functions.R",chdir=TRUE)


output.path <- Sys.getenv(c('CALVAD_IMPUTE_PATH'))[1]

localcouch = Sys.getenv(c('CALVAD_LOCAL_COUCH'))[1]
if(is.null(localcouch)){
  localcouch = FALSE
}else{
  localcouch = TRUE
}




district = Sys.getenv(c('CALVAD_DISTRICT'))[1]
if(is.null(district)){
  print('assign a district to the RDISTRICT environment variable')
  exit(1)
}

vds.id = Sys.getenv(c('CALVAD_VDSID'))[1]
if(is.null(vdsfile)){
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

file.names <- strsplit(vdsfile,split="/")
file.names <- file.names[[1]]
fname <-  strsplit(file.names[length(file.names)],"\\.")[[1]][1]
vds.id <-  get.vdsid.from.filename(fname)


impute.vds.site(vds.id,year,vdsfile=vdsfile,district=district)
