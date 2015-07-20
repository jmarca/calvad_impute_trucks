## This is a top-level R script
##
## it will impute VDS data for WIM sites that do not have a pair.
## That is, a remote, isolated WIM site that does not have a nearby
## VDS station is missing volume and occupancy data.  While it does
## have a close proxy in the count information from the summary
## report, sometimes (a lot of times) the summary information is way
## off from reality.
##
## This script will call a Amelia such that the missing volume and
## occupancy (n,o) for each lane are imputed, by combining the WIM
## site of interest with similar, hopefully near-ish WIM-VDS pair
## sites.  It is to be expected that the WIM-VDS pair sites are far
## from the WIM site in question, because that's what "isolated WIM
## site" means, eh?  So to fight against skewing the data to a single
## VDS-WIM site, two or more VDS-WIM pairs will be used for the
## imputation, thus giving more source material for Amelia to do its
## thing.
##
## typically, you won't call this directly, but rather will call using
## the node.js calling script "lib/trigger_wim_impute_v_o.js", which
## will run this script for all the needed WIM sites.  You can also
## run this script directly, say for testing purposes or to redo a
## single site, but reading through this code and setting the
## appropriate environment variables.  However, it will be difficult
## to set some of them as they require hitting CouchDB and collecting
## data.  Better to try hacking the node.js script to just run for a
## single site or for a particular site

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


## connect to postgresql.  Don't really need this anymore, but it
## can't hurt...would be used if the local file system did not have
## the WIM data maybe?  Better safe than sorry.

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,port=config$postgresql$port
                  ,dbname=config$postgresql$db)


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


wim_path = Sys.getenv(c('CALVAD_WIM_PATH'))[1]
if('' == wim_path){
    if(! is.null(config$calvad$wimpath) && config$calvad$wimpath != '' ){
        wim_path <- config$calvad$wimpath
    }
}
if('' == wim_path){
    print('assign a path to find the imputed WIM data to the CALVAD_WIM_PATH environment variable or set {calvad: {wimpath : /the/wim/path}} in the config file')
    stop(1)
}

output_path <- Sys.getenv(c('CALVAD_OUTPUT_PATH'))[1]
if('' == output_path || is.null(output_path)){
    if(! is.null(config$calvad$outputpath) && config$calvad$outputpath != '' ){
        output_path <- config$calvad$outputpath
    }
}
if('' == output_path || is.null(output_path)){
    print(paste('CALVAD_OUTPUT_PATH environment variable is not set and no entry in config file under calvad:{outputpath:...}, so setting output path to',wim_path))
    output_path <- wim_path
}

image_path <- Sys.getenv(c('CALVAD_IMAGE_PATH'))[1]
if('' == image_path || is.null(image_path)){
    if(! is.null(config$calvad$imagepath) && config$calvad$imagepath != '' ){
        image_path <- config$calvad$imagepath
    }
}
if('' == image_path || is.null(image_path)){
    print(paste('CALVAD_IMAGE_PATH environment variable is not set and no entry in config file under calvad:{imagepath:...}, so setting image path to','images'))
    image_path <- 'images'
}

maxiter = Sys.getenv(c('CALVAD_WIMWIM_IMPUTE_MAXITER'))[1]
if(is.null(maxiter) || is.na(maxiter)){
    maxiter=200
}
maxiter <- as.numeric(maxiter)

wim_pairs = Sys.getenv(c('CALVAD_WIM_PAIRS'))[1]
if(is.null(wim_pairs) || '' == wim_pairs){
    print(paste('assign the wim_pairs for site',wim_site,wim_dir,'to the CALVAD_WIM_PAIRS environment variable',collapse=' '))
    stop(1)
}
## need to convert the CSV string of wim_pairs info into a proper R list
w_p.df <- as.data.frame(matrix(data=strsplit(x=wim_pairs,split=',')[[1]],ncol=3,byrow=TRUE),stringsAsFactors=FALSE)
w_p.df[,1] <- as.numeric(w_p.df[,1])
w_p.df[,2] <- as.numeric(w_p.df[,2])


wim_pairs <- list()
for(i in 1:length(w_p.df[,1])){
    wim_pairs[[i]] <- list(vds_id=w_p.df[i,1],
                           wim_site=w_p.df[i,2],
                           direction=w_p.df[i,3])
}

print(paste('calling impute with options',
            paste(c(site_no=wim_site,
                    direction=wim_dir,
                    wim_pairs=wim_pairs,
                    year=year,
                    wim_path=wim_path,
                    output_path=output_path,
                    maxiter=maxiter,
                    trackingdb=config$couchdb$trackingdb
                    ))))

df.voimputed.agg <- impute.wim.n.o(site_no=wim_site,
                                   wim_dir=wim_dir,
                                   wim_pairs=wim_pairs,
                                   year=year,
                                   wim_path=wim_path,
                                   maxiter=maxiter,
                                   trackingdb=config$couchdb$trackingdb
                                   )
## now the post imputation stuff...plots and CSV dump
result <- post_impute_handling(df.voimputed.agg,
                               wim_site,wim_dir,year,
                               plot_path=image_path,
                               csv_path=output_path,
                               trackingdb=config$couchdb$trackingdb
                               )

print(paste('imputation output saved to',result))
quit(save='no',status=10)
