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
    possible.pairing <-
        calvadrscripts::get.vds.paired.to.wim(year=year,
                                              site_no=wim_site,
                                              direction = wim_dir,
                                              trackingdb=trackingdb)
    df.merged <- NULL
    if(dim(possible.pairing)[1] > 0){
        print('get from couchdb')
        vds_id <- possible.pairing$vds_id
        att_doc <- possible.pairing$doc
        result <- rcouchutils::couch.get.attachment(db=trackingdb,
                                                     docname=vds_id,
                                                    attachment = att_doc)
        nm <- names(result)[1]
        df.merged <- result[[1]][[nm]]
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

    ## and now, continue the same in both cases

    ## add the site id to the data
    df.merged$site_dir <- cdb.wimid

    df.merged.l <- calvadrscripts::transpose.lanes.to.rows(df.merged)

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
                                ,id=cdb.wimid
                                ,doc=list('extract_to_csv'='finished')
                                ,db=trackingdb)

    return (file)

}
