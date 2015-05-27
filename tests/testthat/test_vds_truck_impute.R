config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('vds','wim','impute','calls')
rcouchutils::couch.makedb(parts)
path <- './files'
year <- 2012

file <- './files/wim.51.E.vdsid.318383.2012.paired.RData'
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=318383,
                      file=file)
file <- './files/wim.52.W.vdsid.313822.2012.paired.RData'
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=313822,
                      file=file)


test_that(
    "can impute trucks files",{
    wim.pairs <- list()
    wim.pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
    wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
    bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
                                  vds.nvars=c('nl1','nr3','nr2','nr1'),
                                  year=2012,
                                  db=parts
                                  )

    expect_that(bigdata,is_a('data.frame'))

    expect_that(dim(bigdata),equals(c(10544,49)))
    expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

## a sample output from distance table
##  { vds_id: '311903',
##     site_no: 51,
##     freeway: 50,
##     direction: 'east',
##     dist: 4564.10931177 },
##   { vds_id: '311903',
##     site_no: 52,
##     freeway: 50,
##     direction: 'west',
##     dist: 4564.10931177 },
    year <- 2012

    vds_id <- 311903
    ## load the vds data
    print(paste('loading',vds.id,'from',path))
    df.vds <- get.and.plot.vds.amelia(
                  pair=vds_id,
                  year=year,
                  doplots=FALSE,
                  remote=FALSE,
                  path='./files',
                  force.plot=FALSE,
                  trackingdb=parts)

    ## so that I can pluck out just this site's data at the end of imputation
    df.vds[,'vds_id'] <- vdsid

    ## pick off the lane names so as to drop irrelevant lanes in the loop below
    vds.names <- names(df.vds)
    vds.nvars <- grep( pattern="^n(l|r)\\d+",x=vds.names,perl=TRUE,value=TRUE)

#####################
    ## loading WIM data paired with VDS data from WIM neighbor sites
######################

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
