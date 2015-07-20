##' Wrapper that cleans up amelia output prior to calling the stock
##' condense amelia output routine from calvadrscripts
##'
##' The stock call to calvadrscripts::condense.amelia.output will
##' merge all data based on the timestamp, variable name.  I don't
##' want that here, but rather want to get rid of all the sites that
##' are not the site that I am interested in.  So call this first to
##' drop the data that are from the other sites, prior to calling the
##' standard aggregation function.
##' @title c.a.o_wrapper
##' @param aout the output from Amelia run
##' @param keep_site the value of the "vds_id" variable that
##'     represents the site that you want to keep.
##' @param op the operator to use for aggregation.  Passed on to
##'     condense.amelia.output.
##' @return the aggregated amelia output as a data frame
##' @author James E. Marca
##'
c.a.o_wrapper <- function(aout,keep_site,op=median){


    for(i in 1:length(aout$imputations)){
        aout$imputations[[i]] <- aout$imputations[[i]][aout$imputations[[i]][,'vds_id'] == keep_site,]
    }

    df.agg <- calvadrscripts::condense.amelia.output(aout,op)
    df.agg
}
