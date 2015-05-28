##' Impute trucks for a VDS site
##'
##' pass in a VDS id, a year, and a path to stored data.  The
##' appropriate neighboring WIM-VDS pairs will be loaded for this VDS
##' site, and will be used to impute the most likely truck variables
##' given the VDS site's observed volumes and occupancies.
##'
##' @title impute.vds.site
##' @param vdsid the VDS id
##' @param year
##' @param path the root for the local filesystem data
##' @param maxiter maximum iterations for Amelia run
##' @param trackingdb The couchdb tracking db.  Any issues will be
##' noted here
##' @return nothing.  A big quit() from R when complete Run this for
##' the side effects of generating imputed trucks.
##' @author James E. Marca
impute.vds.site <- function(vdsid,wim_sites,year,path,maxiter,trackingdb){

    print(paste('processing ',paste(vdsid,collapse=', ')))


    ## if no neighbors, die now
    if(length(wim_sites)<1){
        print('no neighbors?')
        print(paste(wim_sites,sep=' '))
        quit(status=9)
    }

    ## load the vds data
    print(paste('loading',vds.id,'from',path))
    df.vds <- calvadrscripts::get.and.plot.vds.amelia(
                  pair=vds_id,
                  year=year,
                  doplots=FALSE,
                  remote=FALSE,
                  path=path,
                  force.plot=FALSE,
                  trackingdb=trackingdb)

    ## so that I can pluck out just this site's data at the end of imputation
    df.vds[,'vds_id'] <- vds_id

    ## pick off the lane names so as to drop irrelevant lanes in the loop below
    vds.names <- names(df.vds)
    vds.nvars <- grep( pattern="^n(l|r)\\d+",x=vds.names,perl=TRUE,value=TRUE)

#####################
    ## loading WIM data paired with VDS data from WIM neighbor sites
######################
    bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=vds.nvars,
                                  year=year,
                                  db=trackingdb
                                  )


    if(dim(bigdata)[1] < 100){
        print('bigdata looking pretty empty')
        stop()
    }

    wimsites.names <-  names(bigdata)
    vds.names <- names(df.vds)
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
    miss.names.vds <- union(miss.names.vds,c('vds_id'))
    i.hate.r <- c(miss.names.vds,'nr1') ## need a dummy index or R will simplify
    holding.pattern <- bigdata[,i.hate.r]

    this.vds <- bigdata['vds_id'] == vds_id
    ## this.vds <- !is.na(this.vds)  ## lordy I hate when NA isn't falsey

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

    itercount <- store.amelia.chains(big.amelia,year,vdsid,'truckimputation',maxiter=maxiter)


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
    ## get rid of stray dots in variable names
    db.legal.names  <- gsub("\\.", "_", names(df.amelia.c))
    names(df.amelia.c) <- db.legal.names

    ## compress amelia output into medians, save that.  Much smaller

    df.agg.amelia <- calvadrscripts::wim.medianed.aggregate.df(morebig.amelia)

    ## unsure about this.  seems like lots of NA values could likely be produced.
    df.amelia.c.l <- transpose.lanes.to.rows(df.amelia.c)

    ## okay, actually write the csv file
    filename <- paste('vds_id',vdsid,'truck.imputed',year,'csv',sep='.')
    ## don't prior imputations
    exists <- dir(output.path,filename)
    tick <- 0
    while(length(exists)==1){
        tick = tick+1
        filename <- paste('vds_id',vdsid,'truck.imputed',year,tick,'csv',sep='.')
        ## don't overwrite files
        exists <- dir(output.path,filename)
    }
    file <- paste(output.path,filename,sep='/')

    write.csv(df.amelia.c.l,file=file,row.names = FALSE)
    ## run perl code to slurp output
    ## system2('perl',paste(' -w /home/james/repos/bdp/parse_imputed_vds_trucks_to_couchDB.pl --cdb=imputed/breakup/ --file=',file,sep='')
    ##         ,stdout = FALSE, stderr = paste(output.path,paste(vdsid,year,'parse_output.txt',sep='.'),sep='/'),wait=FALSE)

    ## while that runs, make some plots
    df.amelia.c$vds_id <- NULL
    ## generate a df for plots.  Use median here, because that is what I will do with final output

    df.amelia.zoo <- medianed.aggregate.df(df.amelia.c)
    df.med <- unzoo.incantation(df.amelia.zoo)
    rm(df.amelia.zoo)
    make.truck.plots(df.med,year,vdsid,'vds',vdsid,imputed=TRUE)
    rm(df.med)

    ## again, a save on a remote stystem is useles.  move on to csv,
    ## possibly push to psql or couchdb

    make.truck.plots.by.lane(df.amelia.c.l,year,vdsid,'vds',vdsid,imputed=TRUE)
    quit(save='no',status=10)

}
