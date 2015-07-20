##' Impute best estimate of truck counts and features.
##'
##' This function takes the merged VDS and WIM sets
##'
##' @param df the merged VDS and WIM input data
##' @param count.pattern how to identify the count variables.  Default
##'     is:
##'     "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
##' @param mean.pattern how to identify the "mean" variables.  Not to
##'     be taken literally.  This is a pattern to identify variables
##'     that aren't count variables, but that are included in the
##'     imputation. For example, occupancy.  Default is "(^ol|^or\\d)"
##' @param exclude.pattern a pattern to identify variables that should
##'     be excluded from the imputation.  The default is
##'     "^(mean|mt_|tr_)"
##' @param maxiter the maximum number of iterations before Amelia
##'     should quit.  Default is 200, but seriously it is pretty rare
##'     when iterating 200 times will produce a usable outcome.  On
##'     the other hand, there are times when iteraing say, 100 times
##'     does.  So set lower to run faster through better behaved data,
##'     and then increase to catch some that require a little more
##'     time.
##' @return the Amelia output
##'
fill.truck.gaps <- function(df
                            ,count.pattern = "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
                            ,mean.pattern="(^ol|^or\\d)"
                            ,exclude.pattern="^(mean|mt_|tr_)"
                            ,maxiter=200
                            ){

  ## truncate negative values to zero
  negatives <-  !( df>=0 | is.na(df) )
  df[negatives] <- 0

  print('Imputation step; filling in the truck NAs in the data:')
  print(summary(df))

  ## sort out upper limits
  occ.pattern <- "(^ol1$|^or\\d$)"
  ic.names <- names(df)
  ic.names <- grep( pattern=exclude.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=TRUE)
  sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
  ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)
  occ.vars <-  grep( pattern=occ.pattern,x=mean.vars ,perl=TRUE,value=TRUE)
  M <- 10000000000  #arbitrary bignum
  ic.names <- names(df)
  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]
  max.vals <- apply( df[,count.vars], 2, max ,na.rm=TRUE)
  pos.bds <- cbind(pos.count,0,1.10*max.vals)
  ## limit the mean vars less, but exclude occ vars
  if(length(setdiff(mean.vars,occ.vars))>0){
    pos.count <- (1:length(ic.names))[is.element(ic.names, setdiff(mean.vars,occ.vars))]
    pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))
  }
  ## now limit occupancy to (0,1)
  pos.count <- (1:length(ic.names))[is.element(ic.names, occ.vars)]
  pos.bds <- rbind(pos.bds,cbind(pos.count,0,1))

  print("bounds:")
  print(pos.bds)

  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))
  exclude.as.id.vars <- union(exclude.as.id.vars,'vds_id')
  print(paste("count vars:",paste(count.vars,collapse=' ')))
  print(paste("mean vars:", paste(mean.vars,collapse=' ')))
  print(paste("excluded:",  paste(exclude.as.id.vars,collapse=' ')))
  ## this version is thorough, but does not run fast enough for the moment
  ## df.amelia <-
  ##   Amelia::amelia(df,idvars=exclude.as.id.vars,
  ##          ts="tod",
  ##          splinetime=6,
  ##          lags =count.vars,leads=count.vars,
  ##          sqrts=count.vars,
  ##          cs="day",
  ##          intercs=TRUE,
  ##          emburn=c(2,maxiter),
  ##          bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df))
  ## this one works
  ## df.amelia <-
  ##   Amelia::amelia(df,idvars=exclude.as.id.vars,
  ##          ts="tod",
  ##          splinetime=3,
  ##          ## lags =count.vars,leads=count.vars,
  ##          sqrts=count.vars,
  ##          cs="day",
  ##          intercs=TRUE,
  ##          emburn=c(2,maxiter),
  ##          bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df)
  ##          )

    ## this one is going for speed
  ## df$toddow <- 24 * df$day + df$tod
  ## exclude.as.id.vars <- c('tod','day',exclude.as.id.vars)
  ##   df.amelia <-
  ##   Amelia::amelia(df,idvars=exclude.as.id.vars,
  ##          ts="toddow",
  ##          splinetime=6,
  ##          #lags =count.vars,
  ##          #leads=count.vars,
  ##          sqrts=count.vars,
  ##          #cs="day",
  ##          #intercs=TRUE,
  ##          emburn=c(2,maxiter),
  ##          bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df)
  ##          ##,m=1 ## desperate measures!  set to limit the imputations
  ##          )


 df.amelia <-
    Amelia::amelia(df,idvars=exclude.as.id.vars,
           ts="tod",
           splinetime=6,
#           lags =count.vars,leads=count.vars,
           sqrts=count.vars,
           cs="day",
           intercs=TRUE,
           emburn=c(2,maxiter),
           bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df))
  print('done imputing run')
    df.amelia
}
