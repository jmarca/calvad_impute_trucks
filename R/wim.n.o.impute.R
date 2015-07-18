##' Impute volume and occupancy for a WIM site
##'
##' pass in a WIM site, a direction, a year, and a path to stored data.  The
##' appropriate neighboring WIM-VDS pairs will be loaded for this VDS
##' site, and will be used to impute the most likely truck variables
##' given the VDS site's observed volumes and occupancies.
##'
##' @title impute.wim.n.o
##' @param vdsid the VDS id for the site where you want to impute
##'     truck data
##'
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
##' @param vds_path where to start looking for VDS data
##' @param output_path where to write the output
##' @param maxiter maximum iterations for Amelia run
##' @param trackingdb The couchdb tracking db.  Will fetch the WIM-VDS
##'     paired data from this database, and any issues will be noted
##'     here
##'
##' @return the file name for the dumped CSV file containing the
##'     results of the imputation
##' @author James E. Marca
##' @export
##'
impute.wim.n.o <- function(site_no,wim_dir,wim_pairs,year,
                           wim_path,
                           output_path,
                           maxiter,
                           trackingdb,
                           force.plot=TRUE){

    print('hello from impute.wim.n.o')
    cdb.wimid <- paste('wim',site_no,wim_dir,sep='.')
    print(paste('processing ',cdb.wimid,'paired to',
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
    df.wim.imputed[,'cdb.wimid'] <- cdb.wimid

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
        print('names in wim, not in bigdata')
        print(paste(miss.names.bigdata,collapse=', ',sep=', '))
        bigdata[,miss.names.bigdata] <- NA
    }
    ## of course this will be necessary, as the bigdata sets have truck
    ## and vol/occ, and the wim site has no vol occ
    print('names in bigdata, not in wim')
    print(paste(miss.names.wim,collapse=', ',sep=', '))
    df.wim.imputed[,miss.names.wim] <- NA

    ## merge vds into bigdata
    bigdata <- rbind(bigdata,df.wim.imputed)
    ## miss.names.vds <- union(miss.names.vds,c('vds_id'))
    i.hate.r <- c(miss.names.wim,'heavyheavy_r1') ## need a dummy index or R will simplify
    holding.pattern <- bigdata[,i.hate.r]

    this.site <- bigdata['cdb.wimid'] == cdb.wimid


    ## exclude as id vars for now, okay?? test and see
    for(i in c('cdb.wimid','vds_id','obs_count')){
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
     ## big.amelia <- fill.truck.gaps(bigdata,maxiter=maxiter)

    ## write out the imputation chains information to couchdb for later analysis
    ## and also generate plots as attachments


    ## extract just this vds_id data and
    ## put back any variables I took out above

    df.amelia.c <- big.amelia$imputations[[1]][this.vds,]

    if(length(miss.names.vds)>0){
        df.amelia.c[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]
    }

    ## limit to what I did impute only
    varnames <- names(df.amelia.c)
    var.list <- names.munging(varnames)
    keep.names <- setdiff(varnames,var.list$exclude.as.id.vars)
    keep.names <- union(keep.names,c('ts','tod','day','vds_id'))
    keep.names <- setdiff(keep.names,names_o_n)

    df.amelia.c <- df.amelia.c[,keep.names]

    if(length(big.amelia$imputations) > 1){
        for(i in 2:length(big.amelia$imputations)){
            temp <- big.amelia$imputations[[i]][this.vds,]
            if(length(miss.names.vds)>0){
                temp[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]
            }
            temp <- temp[,keep.names]
            df.amelia.c <- rbind(df.amelia.c,temp)
        }
    }

    df.agg.amelia <- calvadrscripts::wim.medianed.aggregate.df(df.amelia.c)
    attach.files <- calvadrscripts::plot_wim.data(df.merged=df.agg.amelia
                                                 ,site_no=vds_id
                                                 ,direction=''
                                                 ,year=year
                                                 ,fileprefix='vdstruckimpute'
                                                 ,subhead='\nVDS site imputed trucks'
                                                 ,force.plot=force.plot
                                                 ,trackingdb=trackingdb
                                                  )
    if(length(attach.files) != 1){
        for(f2a in c(attach.files)){
            rcouchutils::couch.attach(trackingdb,vds_id,f2a)
        }
    }

    ## a lot of NA values get produced.  whatever. left lane trucks tend not to exist
    ## df.amelia.c.l <- calvadrscripts::transpose.lanes.to.rows(df.amelia.c)
    df.agg.amelia.l <- calvadrscripts::transpose.lanes.to.rows(df.agg.amelia)

    ## okay, actually write the csv file
    filename <- paste('vds_id',vds_id,'truck.imputed',year,'csv',sep='.')
    ## don't clobber prior imputations
    exists <- dir(output_path,filename)
    tick <- 0
    while(length(exists)==1){
        tick = tick+1
        filename <- paste('vds_id',vds_id,'truck.imputed',year,tick,'csv',sep='.')
        ## don't overwrite files
        exists <- dir(output_path,filename)
    }
    file <- paste(output_path,filename,sep='/')


    ## aggregate to median, save as CSV, and/or write to couchdb right
    ## here

    write.csv(df.agg.amelia.l,file=file,row.names = FALSE)
    ## run perl code to slurp output
    ## system2('perl',paste(' -w /home/james/repos/bdp/parse_imputed_vds_trucks_to_couchDB.pl --cdb=imputed/breakup/ --file=',file,sep='')
    ##         ,stdout = FALSE, stderr = paste(output_path,paste(vds_id,year,'parse_output.txt',sep='.'),sep='/'),wait=FALSE)



    ## last thing is to tag the "done state" in couchdb
    calvadrscripts::store.amelia.chains(big.amelia,year,vds_id,'truckimputation',maxiter=maxiter,db=trackingdb)

    return (file)

}
