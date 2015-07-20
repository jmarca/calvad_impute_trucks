##' Impute best estimate of VDS volume and occupancy
##'
##' This function takes the merged VDS and WIM sets and estimates the
##' best guess at the missing volume and occupancy data.
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
fill.vo.gaps <- function(df
                            ,count.pattern = "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed)"
                            ,mean.pattern="(^ol|^or\\d)"
                            ,exclude.pattern="^(mean|mt_|tr_)"
                            ,maxiter=200
                            ){

  ## truncate negative values to zero
  negatives <-  !( df>=0 | is.na(df) )
  df[negatives] <- 0

  print('Imputation step; filling in the truck NAs in the data:')


    limits <- names_munging_for_vo(df=df)


    df.amelia <-
        Amelia::amelia(df,idvars=limits$exclude.as.id.vars,
                       ts="tod",
                       splinetime=6,
                       ##lags =count.vars,leads=count.vars,
                       ## lags =mean.vars,leads=mean.vars,
                       sqrts=limits$count.vars,
                       cs="day",
                       intercs=TRUE,
                       emburn=c(2,maxiter),
                       bounds = limits$pos.bds,
                       max.resample=10,empri = 0.05 *nrow(df))

    print('done imputing run')
    df.amelia
}
