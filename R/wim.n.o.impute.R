##' Impute volume and occupancy for a WIM site
##'
##' pass in a WIM site, a direction, a year, and a path to stored data.  The
##' appropriate neighboring WIM-VDS pairs will be loaded for this VDS
##' site, and will be used to impute the most likely truck variables
##' given the VDS site's observed volumes and occupancies.
##'
##' @title impute.wim.n.o
##' @param site_no the WIM site number
##' @param wim_dir the direction at the site
##' @param wim_pairs the list of "neighboring" WIM sites.  Each entry
##'     in this list should have three components: vds_id, wim_site,
##'     and direction.  For example,
##'
##'     wim_pairs[[1]] <-
##'              list(vds_id=313822,wim_site=52,direction='W')
##'
##'     For the most part, this should be setup by looking at
##'     distances and available data, valid self-imputations, etc.  At
##'     the moment, this is setup in the calling JS code.
##'
##' @param year the year for the analysis
##' @param wim_path the path to start looking for WIM data
##' @param maxiter maximum iterations for Amelia run
##' @param trackingdb The couchdb tracking db.  Will fetch the WIM-VDS
##'     paired data from this database, and any issues will be noted
##'     here
##'
##' @param force.plot
##'
##' @return the file name for the dumped CSV file containing the
##'     results of the imputation
##' @author James E. Marca
##' @export
##'
impute.wim.n.o <- function(site_no,wim_dir,wim_pairs,year,
                           wim_path,
                           maxiter,
                           trackingdb,
                           force.plot=TRUE){

    print('hello from impute.wim.n.o')
    cdb_wimid <- paste('wim',site_no,wim_dir,sep='.')
    print(paste('processing ',cdb_wimid,'paired to',
                paste(wim_pairs,collapse='; '),collapse=' '))

    ## if no neighbors, die now
    if(length(wim_pairs)<1){
        print('no neighboring WIM-VDS pairs passed in?')
        print(paste(wim_pairs,sep=' ',collapse=' '))
        quit(status=9)
    }

    ## load the imputed WIM site data
    df.wim.imputed <-
            calvadrscripts::get.amelia.wim.file.local(site_no=site_no
                                                     ,year=year
                                                     ,direction=wim_dir
                                                     ,path=wim_path)
    df.wim.imputed <- calvadrscripts::wim.medianed.aggregate.df(df.wim.imputed)

    ## so that I can pluck out just this site's data at the end of imputation
    df.wim.imputed[,'vds_id'] <- cdb_wimid

    ## pick off the lane names so as to drop irrelevant lanes in the loop below
    wim.names <- names(df.wim.imputed)

#####################
    ## loading WIM - VDS paired data
######################

    bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim_pairs,
                                  vds.nvars=wim.names,
                                  year=year,
                                  db=trackingdb
                                  )


    if(dim(bigdata)[1] < 100){
        print('bigdata looking pretty empty')
        stop()
    }

    bigdata.names <-  names(bigdata)
    ## vds.names <- names(df.vds)
    miss.names.wim <- setdiff(bigdata.names,wim.names)
    miss.names.bigdata <- setdiff(wim.names,bigdata.names)
    ## could be more lanes at the VDS site, for example
    if(length(miss.names.bigdata)>0){
        bigdata[,miss.names.bigdata] <- NA
    }
    ## of course this will be necessary, as the bigdata sets have truck
    ## and vol/occ, and the wim site has no vol occ
    df.wim.imputed[,miss.names.wim] <- NA

    ## merge vds into bigdata
    bigdata <- rbind(bigdata,df.wim.imputed)

    ## i.hate.r <- c(miss.names.wim,'heavyheavy_r1') ## need a dummy index or R will simplify
    ## holding.pattern <- bigdata[,i.hate.r]

    this.site <- bigdata['vds_id'] == cdb_wimid


    ## exclude as id vars for now, okay?? test and see
    for(i in c('obs_count')){ ## keep vds_id for now
        bigdata[,i] <- NULL
    }

    ## improve imputation?
    ## add volume times occupancy artificial variable now

    occ.pattern <- "^o(l|r)\\d$"
    occ.vars <-  grep( pattern=occ.pattern,x=names(bigdata),perl=TRUE,value=TRUE)
    vol.pattern <- "^n(l|r)\\d$"
    vol.vars <-  grep( pattern=vol.pattern,x=names(bigdata),perl=TRUE,value=TRUE)

    names_o_n <- paste(occ.vars,'times',vol.vars,sep='_')
    bigdata[,names_o_n] <- bigdata[,occ.vars]*(bigdata[,vol.vars])

    ## run amelia to impute missing (trucks)
    print('all set to impute')

    big.amelia <- fill.vo.gaps(bigdata,maxiter=maxiter)
    ## tag the "done state" in couchdb
    calvadrscripts::store.amelia.chains(big.amelia,year,
                                        cdb_wimid,
                                        'vo_imputation',
                                        maxiter=maxiter,
                                        db=trackingdb)


    df.agg.amelia <- c.a.o_wrapper(big.amelia,
                                   cdb_wimid,
                                   op=median)

    return (df.agg.amelia)
}

##' After imputation, do stuff like generate plots and save the CSV.
##'
##' I split this from the impute routine because it makes testing
##' easier.
##' @title post_impute_handling
##' @param aout.agg the amelia output
##' @param site_no the WIM site number
##' @param wim_dir the direction at the site
##' @param year the year
##' @param plot_path where to stick plots before they are written to couchdb
##' @param csv_path where to write the CSV dump of the amelia output
##' @param trackingdb the CouchDB tracking database
##' @return the CSV file name
##' @author James E. Marca
##'
post_impute_handling <- function(aout.agg,site_no,wim_dir,year,
                                 plot_path='./images',
                                 csv_path,trackingdb){

    cdb_wimid <- paste('wim',site_no,wim_dir,sep='.')

    subhead='\npost-imputation data'
    fileprefix='imputed_vo'
    attach.files <- calvadrscripts::plot_vds.data(
        aout.agg,cdb_wimid,year,
        fileprefix,subhead,
        force.plot=TRUE,
        path=plot_path,
        trackingdb=trackingdb)

    if(length(attach.files) != 1){
        for(f2a in c(attach.files)){
            rcouchutils::couch.attach(trackingdb,cdb_wimid,f2a)
        }
    }
    ## aggregate to median, save as CSV, and/or write to couchdb right
    ## here
    filename <- write_csv(cdb_wimid,year,aout.agg,csv_path,trackingdb)

    return (filename)

}
