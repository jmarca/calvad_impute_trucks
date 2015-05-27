config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('vds','impute','calls')
rcouchutils::couch.makedb(parts)

test_that("prelude code to vds impute trucks works okay",{

    ## impute.vds.site <- function(vdsid,wim_sites,year,path,maxiter,trackingdb){

    print(paste('processing ',paste(vdsid,collapse=', ')))


    wim_sites <- c(1,2,3,4,5,6,7) ## blah blah get it right

    ## load the vds data
    print(paste('loading',vds.id,'from',path))
    df.vds <- get.and.plot.vds.amelia(
                  pair=vds.id,
                  year=year,
                  doplots=FALSE,
                  remote=FALSE,
                  path=path,
                  force.plot=FALSE,
                  trackingdb=trackingdb)

    ## so that I can pluck out just this site's data at the end of imputation
    df.vds[,'vds_id'] <- vdsid

    ## pick off the lane names so as to drop irrelevant lanes in the loop below
    vds.names <- names(df.vds)
    vds.nvars <- grep( pattern="^n(l|r)\\d+",x=vds.names,perl=TRUE,value=TRUE)

#####################
    ## loading WIM data paired with VDS data from WIM neighbor sites
######################


    bigdata <- load.wim.pair.data(wim.ids,vds.nvars=vds.nvars,year=year)


    if(dim(bigdata)[1] < 100){
        print('bigdata looking pretty empty')

##couch.set.state(year,vds.id,list('truck_imputation_failed'=paste(dim(bigdata)[1], 'records in wim neighbor sites')),local=localcouch)
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

    ## merge into bigdata
    bigdata <- rbind(bigdata,df.vds)
    miss.names.vds <- union(miss.names.vds,c('vds_id'))
    i.hate.r <- c(miss.names.vds,'nr1') ## need a dummy index or R will simplify
    holding.pattern <- bigdata[,i.hate.r]

    this.vds <- bigdata['vds_id'] == vdsid
    this.vds <- !is.na(this.vds)  ## lordy I hate when NA isn't falsey

    for(i in miss.names.vds){
        bigdata[,i] <- NULL
    }
    rm(df.vds)
    gc()

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

    })

    file  <- './files/1211682_ML_2012.df.2012.RData'
    fname <- '1211682_ML_2012'
    vds.id <- 1211682
    year <- 2012
    seconds <- 120
    path <- '.'
    result <- self.agg.impute.VDS.site.no.plots(fname=fname,
                                                f=file,
                                                path=path,
                                                year=year,
                                                seconds=seconds,
                                                goodfactor=3.5,
                                                maxiter=20,
                                                con=con,
                                                trackingdb=parts)


    expect_that(result,equals(1))
    datfile <- dir(path='.',pattern='vds_hour_agg',full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste('vds_hour_agg',vds.id,sep='.')))

    datfile <- dir(path='.',
                   pattern=paste(vds.id,'.*imputed.RData$',sep=''),
                   full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste(vds.id,
                                         '_ML_',
                                         year,'.',
                                         seconds,'.',
                                         'imputed.RData',
                                         sep='')))

    saved.state <- rcouchutils::couch.check.state(
        year=year,
        id=vds.id,
        'vdsraw_chain_lengths',
        db=parts)
    expect_that(saved.state,is_a('numeric'))
    expect_that(saved.state,
                equals(c(3,3,3,3,3)))

})

unlink('./files/1073210_ML_2012.120.imputed.RData')
test_that("ignoring speed in imputation works okay",{

    file  <- './files/1073210_ML_2012.df.2012.RData'
    fname <- '1073210_ML_2012'
    vds.id <- 1073210
    year <- 2012
    seconds <- 120
    path <- '.'
    result <- self.agg.impute.VDS.site.no.plots(fname=fname,
                                                f=file,
                                                path=path,
                                                year=year,
                                                seconds=seconds,
                                                goodfactor=3.5,
                                                maxiter=20,
                                                con=con,
                                                trackingdb=parts)


    expect_that(result,equals(1))
    datfile <- dir(path='.',pattern='vds_hour_agg',full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste('vds_hour_agg',vds.id,sep='.')))

    datfile <- dir(path='.',
                   pattern=paste(vds.id,'.*imputed.RData$',sep=''),
                   full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste(vds.id,
                                         '_ML_',
                                         year,'.',
                                         seconds,'.',
                                         'imputed.RData',
                                         sep='')))

    saved.state <- rcouchutils::couch.check.state(
        year=year,
        id=vds.id,
        'vdsraw_chain_lengths',
        db=parts)
    expect_that(saved.state,is_a('numeric'))
    expect_that(saved.state,
                equals(c(5,5,5,5,5)))

})


rcouchutils::couch.deletedb(parts)
unlink('./vds_hour_agg.1211682.2012.dat')
unlink('./files/1073210_ML_2012.120.imputed.RData')
unlink('./vds_hour_agg.1073210.2012.dat')
