##' Impute trucks for a VDS site
##'
##' pass in a VDS id, a year, and a path to stored data.  The
##' appropriate neighboring WIM-VDS pairs will be loaded for this VDS
##' site, and will be used to impute the most likely truck variables
##' given the VDS site's observed volumes and occupancies.
##'
##' @title impute.vds.site
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
impute.vds.site <- function(vds_id,wim_pairs,year,
                            vds_path,
                            output_path,
                            maxiter,
                            trackingdb,
                            force.plot=TRUE){

    print('hello from impute.vds.site')
    print(paste('processing ',paste(vds_id,collapse=', '),'paired to',
                paste(wim_pairs,collapse='; '),collapse=' '))

    ## if no neighbors, die now
    if(length(wim_pairs)<1){
        print('no neighboring WIM-VDS pairs passed in?')
        print(paste(wim_pairs,sep=' ',collapse=' '))
        quit(status=9)
    }

    ## load the vds data
    print(paste('loading',vds_id,'from',vds_path))
    df.vds <- calvadrscripts::get.and.plot.vds.amelia(
                  pair=vds_id,
                  year=year,
                  doplots=FALSE,
                  remote=FALSE,
                  path=vds_path,
                  force.plot=FALSE,
                  trackingdb=trackingdb)

    ## so that I can pluck out just this site's data at the end of imputation
    df.vds[,'vds_id'] <- vds_id

    print(summary(df.vds))
    ## pick off the lane names so as to drop irrelevant lanes in the loop below
    vds.names <- names(df.vds)

#####################
    ## loading WIM data paired with VDS data from WIM neighbor sites
######################
    bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim_pairs,
                                  vds.nvars=vds.names,
                                  year=year,
                                  db=trackingdb
                                  )


    if(dim(bigdata)[1] < 100){
        print('bigdata looking pretty empty')
        stop()
    }

    wimsites.names <-  names(bigdata)
    ## vds.names <- names(df.vds)
    miss.names.wim <- setdiff(wimsites.names,vds.names)
    miss.names.vds <- setdiff(vds.names,wimsites.names)
    ## could be more lanes at the VDS site, for example
    if(length(miss.names.vds)>0){
        bigdata[,miss.names.vds] <- NA
    }
    ## of course this will be necessary, as the wimsites have truck
    ## data and the vds does not
    df.vds[,miss.names.wim] <- NA

    ## merge vds into bigdata
    bigdata <- rbind(bigdata,df.vds)
    ## miss.names.vds <- union(miss.names.vds,c('vds_id'))
    i.hate.r <- c(miss.names.vds,'nr1') ## need a dummy index or R will simplify
    holding.pattern <- bigdata[,i.hate.r]

    this.vds <- bigdata['vds_id'] == vds_id

    ## exclude as id vars for now, okay?? test and see
    for(i in miss.names.vds){
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

    ## bugfix.  vds amelia runs might have been done with improper
    ## limits on occ.  Very old runs only, but need to fix here
    occ.pattern <- "^o(l|r)\\d$"
    occ.vars <-  grep( pattern=occ.pattern,x=names(bigdata),perl=TRUE,value=TRUE)
    ## truncate mask
    toobig <-  !( bigdata[,occ.vars]<1 | is.na(bigdata[,occ.vars]) )
    bigdata[,occ.vars][toobig] <- 1

    ## run amelia to impute missing (trucks)
    print('all set to impute')

    big.amelia <- fill.truck.gaps(bigdata,maxiter=maxiter)

    ## write out the imputation chains information to couchdb for later analysis
    ## and also generate plots as attachments


    ## extract just this vds_id data and
    ## put back any variables I took out above

    df.amelia.c <- big.amelia$imputations[[1]][this.vds,]
    df.amelia.c[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]

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
            temp[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]
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
