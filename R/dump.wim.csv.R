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
    cdb_wimid <- paste('wim',wim_site,wim_dir,sep='.')

    ## two cases.  A pair, or not.  If a pair, get the paired combo from couchdb
    possible.pairing <-
        calvadrscripts::get.vds.paired.to.wim(year=year,
                                              site_no=wim_site,
                                              direction = wim_dir,
                                              trackingdb=trackingdb)
    df.merged <- NULL
    print(paste(possible.pairing))
    if(dim(possible.pairing)[1] > 0){
        print('get from couchdb')
        df.fake.imputed <- list()
        for(idx in 1:length(possible.pairing$vds_id)){
            print(paste('doc ',idx))
            vds_id <- possible.pairing$vds_id[idx]
            att_doc <- possible.pairing$doc[idx]
            result <- rcouchutils::couch.get.attachment(db=trackingdb,
                                                        docname=vds_id,
                                                        attachment = att_doc)
            nm <- names(result)[1]
            df.fake.imputed$imputations[[idx]] <- result[[1]][[nm]]
            print(summary(df.fake.imputed$imputations[[idx]]))
        }
        if(length(possible.pairing$vds_id) == 1){
            df.merged <- df.fake.imputed$imputations[[1]]
        }else{
            print('combining multiple pairings')
            df.merged <- calvadrscripts::condense.amelia.output(df.fake.imputed,op=mean)
            print(summary(df.merged))
        }
    }else{
        print('get from impute output')
        ## no already-merged pair
        ## get wim self-imputation result
        df.wim.imputed <-
            calvadrscripts::get.amelia.wim.file.local(site_no=wim_site
                                                     ,year=year
                                                     ,direction=wim_dir
                                                     ,path=wim_path)
        ## TODO
        ##
        ## might want to think about imputing most likely n and o
        ## based on other WIM-VDS pairings, using same approach as for
        ## VDS truck imputation. I thought I had code to do that
        ## already, but I can't find it yet in bdp

        if( length(df.wim.imputed) == 1 ){
            print(paste("amelia run for wim not good",df.wim.imputed))
            quit('no',1)
        }


        df.merged <- calvadrscripts::condense.amelia.output(df.wim.imputed)
    }

    print(summary(df.merged))
    ## and now, continue the same in both cases

    filename <-  write_csv(cdb_wimid,year,df.merged,output_path,trackingdb)
    return (filename)
}

##' Write out the wim + vds file as CSV for loading into CouchDB
##'
##' A reusable bit of code that converts the aggregated imputations
##' into CSV dump
##' @title write_csv
##' @param cdb_wimid the couchdb wim id (site number + direction
##' @param year the year
##' @param df.merged the data
##' @param output_path where to write the CSV file
##' @param trackingdb the tracking database couchdb
##' @param name_prefix an optional variation on the name
##' @return the name (and path) of the created file
##' @author James E. Marca
##'
write_csv <- function(cdb_wimid,year,df.merged,output_path,trackingdb,name_prefix='truck.imputed'){
    ## add the site id to the data
    df.merged$site_dir <- cdb_wimid
    print(names(df.merged))
    df.merged.l <- calvadrscripts::transpose.lanes.to.rows(df.merged)
    print(names(df.merged.l))
    keepnames <- c('ts','site_dir','tod','day','n','o','not_heavyheavy','heavyheavy',
                   'hh_weight','hh_axles','hh_speed','nh_weight',
                   'nh_axles','nh_speed',
                   'wgt_spd_all_veh_speed','count_all_veh_speed',
                   'lane')

    extra_names <- setdiff (keepnames,names(df.merged.l))
    if( length(extra_names) > 0 ){
        print('extra names in keepnames??')
        print(extra_names)
        stop()
    }
    ## save to csv
    filename <- paste(cdb_wimid,name_prefix,year,'csv',sep='.')
    ## don't clobber prior imputations
    exists <- dir(output_path,filename)
    tick <- 0
    while(length(exists)==1){
        tick = tick+1
        filename <- paste(cdb_wimid,name_prefix,year,tick,'csv',sep='.')
        ## don't overwrite files
        exists <- dir(output_path,filename)
    }
    file <- paste(output_path,filename,sep='/')

    write.csv(df.merged.l[,keepnames],file=file,row.names = FALSE)

    rcouchutils::couch.set.state(year=year
                                ,id=cdb_wimid
                                ,doc=list('extract_to_csv'='finished')
                                ,db=trackingdb)

    return (file)
}
