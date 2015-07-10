    res <- couch.allDocs(dbname,)

    pairdocs <- rcouchutils::couch.allDocs(db=trackingdb
                                          ,query=list(
                                               'startkey'=paste('%5b',year,'%2c%22',cdb.wimid,'%22%5d',sep='')
                                               'endkey'=paste('%5b',year,'%2c%22',cdb.wimid,'\ufff0%22%5d',sep='')
                                              ,'reduce'='false')
                                          ,view='_design/vds/_view/pairRData'
                                      ,include.docs = FALSE)
    ## dissect the response
    rows <- docs$rows
    records <- sapply(rows,function(r){
        ## parse out wim info
        x = r$key[[3]]
        m <- regexec("^wim\\.([0-9]+)\\.([NSEW])",x)
        wim.info <- regmatches(x,m)[[1]]
        return (list('year'=as.numeric(r$key[[1]]),
                     'vds_id'=as.numeric(r$key[[2]]),
                     'doc'=r$key[[3]],
                     'wim_id'=as.integer(wim.info[2]),
                     'direction'=wim.info[3]
                     ))
    })
    if(length(records)==0){
        return(data.frame())
    }


##' Dump WIM imputation data to CSV file
##'
##' WIM sites can be either paired with a VDS site or not.  They need
##' to get dumped to CSV as well for counting vehicle types.
##'
##' @title dump.wim.csv
##' @param wim_site The WIM site number.  Just the number
##' @param wim_dir The direction.  N, S, E, W
##' @param year The year
##' @param wim_path A path to the WIM imputation output
##' @param output_path Where to save the CSV file
##' @param trackingdb The couchdb tracking db for imputation status
##' @return the file name
##' @author James E. Marca
##' @export
##'
dump.wim.csv <- function(wim_site,wim_dir,year,
                         wim_path,output_path,
                         trackingdb){

    print(paste('processing',wim_site,wim_dir))
    cdb.wimid <- paste('wim',wim_site,wim_dir,sep='.')

    ## two cases.  A pair, or not.  If a pair, get the paired combo from couchdb

    ## get wim self-imputation result
    df.wim.imputed <- calvadrscripts::get.amelia.wim.file.local(site_no=wim_site
                                                               ,year=year
                                                               ,direction=wim_dir
                                                               ,path=wim_path)

    if( length(df.wim.imputed) == 1 ){
        print(paste("amelia run for wim not good",df.wim.imputed))
        quit('no',1)
    }


    df.merged <- calvadrscripts::condense.amelia.output(df.wim.imputed)

    ## add the site id to the data
    df.merged$site_dir <- cdb.wimid

    df.merged.l <- transpose.lanes.to.rows(df.merged)

    keepnames <- c('tod','day','ts','site_dir','lane','heavyheavy',
                   'hh_weight','hh_axles','hh_speed','nh_weight',
                   'nh_axles','nh_speed',
                   'wgt_spd_all_veh_speed','count_all_veh_speed',
                   'not_heavyheavy')

    ## save to csv
    filename <- paste(cdb.wimid,'truck.imputed',year,'csv',sep='.')
    file <- paste(output_path,filename,sep='/')
    write.csv(df.merged.l[,keepnames],file=file,row.names = FALSE)

    rcouchutils::couch.set.state(year=year
                                ,detector.id=cdb.wimid
                                ,doc=list('extract_to_csv'='finished'))

    return (file)

}
