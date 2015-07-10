## need node_modules directories
dot_is <- getwd()
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(node_paths, winslash = "/", mustWork = FALSE)
lib_paths <- .libPaths()
.libPaths(c(path, lib_paths))

print(.libPaths())

pkg <- devtools::as.package('.')
ns_env <- devtools::load_all(pkg,quiet = TRUE)$env

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
                  ,port=config$postgresql$port
                  ,dbname=config$postgresql$db)

wim_site <- Sys.getenv(c('WIM_SITE'))[1]
if('' ==  wim_site){
  print('assign a valid site to the WIM_SITE environment variable')
  stop(1)
}
wim_site <- as.numeric(wim_id)


year = as.numeric(Sys.getenv(c('CALVAD_YEAR'))[1])
if('' == year){
  print('assign the year to process to the CALVAD_YEAR environment variable')
  stop(1)
}

wim_path = Sys.getenv(c('CALVAD_WIM_PATH'))[1]
if('' == wim_path){
    if(! is.null(config$calvad$wimpath) && config$calvad$wimpath != '' ){
        wim_path <- config$calvad$wimpath
    }
}
if('' == wim_path){
    print('assign a path to find the imputed wim data to the CALVAD_WIM_PATH environment variable or set calvad: {wimpath : /the/wim/path} in the config file')
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


keepnames <- c('tod','day','ts','site_dir','lane','heavyheavy','hh_weight','hh_axles','hh_speed','nh_weight','nh_axles','nh_speed','wgt_spd_all_veh_speed','count_all_veh_speed','not_heavyheavy')

for (year in years ){
  print(year)
  files <- get.filenames(server=server,service='wimdata',base.dir=year,pattern='wim.*imputed.*RData$')

  for(file in files[1:length(files)]){
    print(file)
    wim.imputed <-get.wim.file(file)
    load.result <- load(file=wim.imputed)
    pos <- regexpr("wim.\\d*",file)
    wim.dir <- strsplit(file,'/')[[1]][c(2,3)]
    cdb.wimid <- paste('wim',wim.dir[1],wim.dir[2],sep='.')

    to.do <- couch.check.state(year
                               ,cdb.wimid
                               ,process='extract_to_csv')
    if(to.do != 'todo'){
      next; ## comment out to redo everything
    }
    couch.set.state(year=year
                    ,detector.id=cdb.wimid
                    ,doc=list('extract_to_csv'='inprocess'))

    if(length(df.wim.amelia) == 1){
      print(paste("amelia run for WIM site",wim.dir," not good",df.wim.amelia))
      next
    }else if(!length(df.wim.amelia)>0 || !length(df.wim.amelia$imputations)>0 || df.wim.amelia$code!=1 ){
      print(paste("amelia run for WIM site",wim.dir,"not good"))
      next
    }

    itercount <- store.amelia.chains(df.wim.amelia,year,cdb.wimid)
    if(itercount>1){
      couch.set.state(year=year
                      ,detector.id=cdb.wimid
                      ,doc=list('extract_to_csv'='problem'))
      ## next;
    }

    ## use zoo to combine a mean value
    df.wim.amelia.c <- df.wim.amelia$imputations[[1]]
    if(length(df.wim.amelia$imputations) >1){
      for(i in 2:length(df.wim.amelia$imputations)){
        df.wim.amelia.c <- rbind(df.wim.amelia.c,df.wim.amelia$imputations[[i]])
      }
    }
    df.merged <- medianed.aggregate.df(df.wim.amelia.c)
    ts.ts <- unclass(time(df.merged))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
    keep.columns <-  grep( pattern="(^ts|^day|^tod|^obs_count)",x=names(df.merged),perl=TRUE,value=TRUE,invert=TRUE)
    df.merged <- data.frame(coredata(df.merged[,keep.columns]))
    df.merged$ts <- ts.ts
    ts.lt <- as.POSIXlt(df.merged$ts)
    df.merged$tod   <- ts.lt$hour + (ts.lt$min/60)
    df.merged$day   <- ts.lt$wday

    ## add the site id to the data
    df.merged$site_dir <- cdb.wimid

    db.legal.names  <- gsub("\\.", "_", names(df.merged))
    names(df.merged) <- db.legal.names

    df.merged.l <- transpose.lanes.to.rows(df.merged)

    ## save to csv
    sfilename <- paste(cdb.wimid,'truck.imputed',year,'csv',sep='.')
    sfile <- paste(output.path,sfilename,sep='/')
    write.csv(df.merged.l[,keepnames],file=sfile,row.names = FALSE)

    ## run perl code to slurp output into couchdb
    system2('perl',paste(' -w /home/james/repos/bdp/parse_imputed_vds_trucks_to_couchDB.pl --cdb=imputed/breakup/ --file=',sfile,sep='')
          ,stdout = FALSE, stderr = paste(output.path,paste(cdb.wimid,year,'parse_output.txt',sep='.'),sep='/'),wait=FALSE)

    couch.set.state(year=year
                    ,detector.id=cdb.wimid
                    ,doc=list('extract_to_csv'='finished'))

  }
}

dbDisconnect(con)
