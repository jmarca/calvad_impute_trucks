config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('vds','wim','impute','calls')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
year <- 2012
maxiter <- 200

force.plot <- TRUE

file <- paste(path,'wim.51.E.vdsid.318383.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=318383,
                      file=file)
file <- paste(path,'wim.52.W.vdsid.313822.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=313822,
                      file=file)

vds_id <- 311903

## test_that(
##     "can impute trucks files",{
##     wim.pairs <- list()
##     wim.pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
##     wim.pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
##     bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
##                                   vds.nvars=c('nl1','nr3','nr2','nr1'),
##                                   year=2012,
##                                   db=parts
##                                   )

##     expect_that(bigdata,is_a('data.frame'))
##     expect_that(dim(bigdata),equals(c(10544,51)))
##     expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

## ## a sample output from distance table
## ##  { vds_id: '311903',
## ##     site_no: 51,
## ##     freeway: 50,
## ##     direction: 'east',
## ##     dist: 4564.10931177 },
## ##   { vds_id: '311903',
## ##     site_no: 52,
## ##     freeway: 50,
## ##     direction: 'west',
## ##     dist: 4564.10931177 },

##     ## load the vds data
##     print(paste('loading',vds_id,'from',path))
##     df.vds <- calvadrscripts::get.and.plot.vds.amelia(
##                   pair=vds_id,
##                   year=year,
##                   doplots=FALSE,
##                   remote=FALSE,
##                   path=path,
##                   force.plot=FALSE,
##                   trackingdb=parts)

##     ## so that I can pluck out just this site's data at the end of imputation
##     df.vds[,'vds_id'] <- vds_id

##     ## pick off the lane names so as to drop irrelevant lanes in the loop below
##     vds.names <- names(df.vds)

## #####################
##     ## loading WIM data paired with VDS data from WIM neighbor sites
## ######################
##     bigdata <- calvadmergepairs::load.wim.pair.data(wim.pairs=wim.pairs,
##                                   vds.nvars=vds.names,
##                                   year=year,
##                                   db=parts
##                                   )

##     expect_that(bigdata,is_a('data.frame'))
##     expect_that(dim(bigdata),equals(c(10544,37))) ## missing one lane from earlier
##     expect_that(sort(unique(bigdata$vds_id)),equals(c(313822,318383)))

##     ## because there are only three lanes at the VDS site, and 4 lanes
##     ## at the WIM site, I expect that the imputation after merge will
##     ## only have three lanes, that is, l1, r2, r1.  So if I see any r3
##     ## type lanes, fail this test.
##     varnames <- names(bigdata)
##     expect_that(length(grep(pattern='_r3$',x=bigdata,perl=TRUE)),equals(0))


##     wimsites.names <-  names(bigdata)
##     ## vds.names <- names(df.vds)
##     miss.names.wim <- setdiff(wimsites.names,vds.names)
##     miss.names.vds <- setdiff(vds.names,wimsites.names)
##     ## could be more lanes at the VDS site, for example
##     if(length(miss.names.vds)>0){
##         bigdata[,miss.names.vds] <- NA
##     }
##     ## of course this will be necessary, as the wimsites have truck
##     ## data and the vds does not
##     df.vds[,miss.names.wim] <- NA

##     ## merge vds into bigdata
##     bigdata <- rbind(bigdata,df.vds)
##     miss.names.vds <- union(miss.names.vds,c('vds_id'))
##     i.hate.r <- c(miss.names.vds,'nr1') ## need a dummy index or R will simplify
##     holding.pattern <- bigdata[,i.hate.r]

##     this.vds <- bigdata['vds_id'] == vds_id
##     ## this.vds <- !is.na(this.vds)  ## lordy I hate when NA isn't falsey

##     ## exclude as id vars for now, okay?? test and see
##     ## for(i in miss.names.vds){
##     ##     bigdata[,i] <- NULL
##     ## }

##     ## improve imputation?
##     ## add volume times occupancy artificial variable now

##     occ.pattern <- "^o(l|r)\\d$"
##     occ.vars <-  grep( pattern=occ.pattern,x=names(bigdata),perl=TRUE,value=TRUE)
##     vol.pattern <- "^n(l|r)\\d$"
##     vol.vars <-  grep( pattern=vol.pattern,x=names(bigdata),perl=TRUE,value=TRUE)

##     names_o_n <- paste(occ.vars,'times',vol.vars,sep='_')
##     bigdata[,names_o_n] <- bigdata[,occ.vars]*(bigdata[,vol.vars])

##     ## bugfix.  vds amelia runs might have been done with improper
##     ## limits on occ.  Very old runs only, but need to fix here
##     occ.pattern <- "^o(l|r)\\d$"
##     occ.vars <-  grep( pattern=occ.pattern,x=names(bigdata),perl=TRUE,value=TRUE)
##     ## truncate mask
##     toobig <-  !( bigdata[,occ.vars]<1 | is.na(bigdata[,occ.vars]) )
##     bigdata[,occ.vars][toobig] <- 1

##     ## run amelia to impute missing (trucks)
##     print('all set to impute')

##     big.amelia <- fill.truck.gaps(bigdata,maxiter=maxiter)

##     df.amelia.c <- big.amelia$imputations[[1]][this.vds,]
##     df.amelia.c[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]

##     ## limit to what I did impute only
##     varnames <- names(df.amelia.c)
##     var.list <- names.munging(varnames)
##     keep.names <- setdiff(varnames,var.list$exclude.as.id.vars)
##     keep.names <- union(keep.names,c('ts','tod','day','vds_id'))
##     keep.names <- setdiff(keep.names,names_o_n)


##     df.amelia.c <- df.amelia.c[,keep.names]

##     if(length(big.amelia$imputations) > 1){
##         for(i in 2:length(big.amelia$imputations)){
##             temp <- big.amelia$imputations[[i]][this.vds,]
##             temp[,miss.names.vds] <- holding.pattern[this.vds,miss.names.vds]
##             temp <- temp[,keep.names]
##             df.amelia.c <- rbind(df.amelia.c,temp)
##         }
##     }

##     df.agg.amelia <- calvadrscripts::wim.medianed.aggregate.df(df.amelia.c)
##     attach.files <- calvadrscripts::plot_wim.data(df.merged=df.agg.amelia
##                                                  ,site_no=vds_id
##                                                  ,direction=''
##                                                  ,year=year
##                                                  ,fileprefix='vdstruckimpute'
##                                                  ,subhead='\nVDS site imputed trucks'
##                                                  ,force.plot=force.plot
##                                                  ,trackingdb=parts
##                                                   )
##     if(length(attach.files) != 1){
##         for(f2a in c(attach.files)){
##             rcouchutils::couch.attach(parts,vds_id,f2a)
##         }
##     }

##     df.agg.amelia.l <- calvadrscripts::transpose.lanes.to.rows(df.agg.amelia)

##     ## okay, actually write the csv file
##     filename <- paste('vds_id',vds_id,'truck.imputed',year,'csv',sep='.')
##     ## don't clobber prior imputations
##     exists <- dir(output_path,filename)
##     tick <- 0
##     while(length(exists)==1){
##         tick = tick+1
##         filename <- paste('vds_id',vds_id,'truck.imputed',year,tick,'csv',sep='.')
##         ## don't overwrite files
##         exists <- dir(output_path,filename)
##     }
##     file <- paste(output_path,filename,sep='/')

##     write.csv(df.agg.amelia.l,file=file,row.names = FALSE)

##     ## load the file and check that it is okay?


##})


test_that(
    "can impute trucks files with one call",{
        wim_pairs <- list()
        wim_pairs[[1]] <- list(vds_id=313822,wim_site=52,direction='W')
        wim_pairs[[2]] <- list(vds_id=318383,wim_site=51,direction='E')
        result <- impute.vds.site(vds_id=vds_id,
                                  wim_pairs=wim_pairs,
                                  year=year,
                                  vds_path=path,
                                  output_path=path,
                                  maxiter=200,
                                  trackingdb=parts)
        ## ## second time through, expect that the filename has been incremented to 1
        ## testthat::expect_match(result,'truck.imputed.2012.1.csv$')
        ## result <- impute.vds.site(vds_id=vds_id,
        ##                           wim_pairs=wim_pairs,
        ##                           year=year,
        ##                           vds_path=path,
        ##                           output_path=path,
        ##                           maxiter=200,
        ##                           trackingdb=parts)
        ## ## second time through, expect that the filename has been incremented to 2
        ## testthat::expect_match(result,'truck.imputed.2012.2.csv$')

        saved_chain_lengths <- rcouchutils::couch.check.state(year=year,id=311903,state='truckimputation_chain_lengths',db=parts)
        saved_max_iterations <- rcouchutils::couch.check.state(year=year,id=311903,state='truckimputation_max_iterations',db=parts)
        testthat::expect_more_than(object=mean(saved_chain_lengths),expected=50)
        testthat::expect_less_than(object=mean(saved_chain_lengths),expected=80)
        testthat::expect_equal(object=saved_max_iterations,expected=0)

        ## expect that the six post-imputation plots are there too
        doc <- rcouchutils::couch.get(parts,vds_id)
        attachments <- doc[['_attachments']]
        testthat::expect_that(attachments,testthat::is_a('list'))
        testthat::expect_that(sort(names(attachments)),
                              testthat::equals(
                                  c(paste(vds_id,year,
                                          'vdstruckimpute',
                                          c("001.png",
                                            "002.png",
                                            "003.png",
                                            "004.png",
                                            "005.png",
                                            "006.png"),
                                          sep='_')
                                    ))
                              )

        if(requireNamespace("readr", quietly = TRUE)){
            csvdf <- readr::read_csv(
                file=paste(path,'vds_id.311903.truck.imputed.2012.1.csv',sep='/'),
                col_types='ciiiddddcdddddddd')
            csvdf$ts <- readr::parse_datetime(csvdf$ts,"%Y-%m-%d %H:%M:%S",tz="UTC")
            expect_that(table(csvdf$lane)[['l1']],equals(8784))
            expect_that(table(csvdf$lane)[['r1']],equals(8784))
            expect_that(table(csvdf$lane)[['r2']],equals(8784))
            expect_that(levels(as.factor(csvdf$lane)),equals(c('l1','r1','r2')))
            expect_that(dim(csvdf),equals(c(26352,17)))
        }
    })


unlink(paste(path,'/vds_id.',vds_id,'.truck.imputed.',year,'.',c(1,2,3),'.csv',sep=''))
unlink(paste(path,'/vds_id.',vds_id,'.truck.imputed.',year,'.csv',sep=''))
unlink(paste(path,'/images'),recursive = TRUE)
rcouchutils::couch.deletedb(parts)
