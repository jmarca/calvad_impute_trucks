##' process variable names in preparation for Amelia call
##'
##' I split this out so that I could test different things
##'
##' @param names.df the dataframe's names
##' @param count.pattern how to recognize the count variables, default is
##' "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
##' @param mean.pattern how to recognize what I used to call mean
##' variables but now that I by default ignore mean variables, there
##' you go.  Default is "(^ol|^or\\d)" which will grab occupancy
##' variables
##' @param exclude.pattern what variables to exclude from the Amelia
##' run as id variables.  Default is "^(mean|mt_|tr_)"
##' @param df the data frame.  Optional but if you pass it in, then
##' will be used to set up boundaries too
##' @return a list of things,
##' list(exclude.as.id.vars=exclude.as.id.vars,
##'      pos.count =pos.count,
##'      mean.vars =mean.vars,
##'      count.vars=count.vars,
##'      pos.bds   =pos.bds)
##'
##' which is what you need in the above amelia function
names.munging <- function(names.df
                            ,count.pattern = "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
                            ,mean.pattern="(^ol|^or\\d)"
                            ,exclude.pattern="^(mean|mt_|tr_)"
                            ,df=data.frame()
                          ){

  ic.names <- names.df
  occ.pattern <- "(^ol1$|^or\\d$)"
  ic.names <- grep( pattern=exclude.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=TRUE)
  sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
  ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)
  occ.vars <-  grep( pattern=occ.pattern,x=mean.vars ,perl=TRUE,value=TRUE)

  M <- 10000000000  #arbitrary bignum
  ic.names <- names.df

  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]

  pos.bds <- c()
  if(length(df)>0){
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
  }

  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]
  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))

  ##  print(paste("count vars:",paste(count.vars,collapse=' ')))
  ##  print(paste("mean vars:", paste(mean.vars,collapse=' ')))
  list(exclude.as.id.vars=exclude.as.id.vars,
       pos.count=pos.count,
       mean.vars=mean.vars,
       count.vars=count.vars,
       pos.bds=pos.bds
       )
}

##' names munging steps for volume occupancy impute at WIM sites
##'
##' Only real difference I think is the default exclude pattern.  But
##' I'm not sure yet so not going to use the above until I can test
##' that they are the same!
##'
##' @title names_munging_for_vo
##' @param count.pattern a pattern to find count variables
##' @param mean.pattern a pattern to find mean variables (no counts)
##' @param exclude.pattern a pattern to exclude variables from the analysis
##' @param df the data frame
##' @return a list of things,
##' list(exclude.as.id.vars=exclude.as.id.vars,
##'      pos.count =pos.count,
##'      mean.vars =mean.vars,
##'      count.vars=count.vars,
##'      pos.bds   =pos.bds)
##'
##' which is what you need to call the above amelia function
##' @author James E. Marca
##'
names_munging_for_vo <- function(count.pattern= "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed)"
                                ,mean.pattern="(^ol|^or\\d)"
                                ,exclude.pattern="(^mean|^mt_|^tr_|_all_veh_speed)"
                                ,df){


    occ.pattern <- "(^ol1$|^or\\d$)"

    ## sort out upper limits
    ic.names <- names(df)
    ic.names <- grep( pattern=exclude.pattern,
                     x=ic.names ,
                     perl=TRUE,value=TRUE,invert=TRUE)
    sd.vars <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,
                      perl=TRUE,value=TRUE)
    ic.names <- grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,
                     perl=TRUE,value=TRUE,invert=TRUE)

    count.vars <- grep( pattern=count.pattern,x=ic.names,
                       perl=TRUE,value=TRUE)


    not_counts <- grep( pattern=count.pattern,x=ic.names,
                       perl=TRUE,value=TRUE,invert=TRUE)
    mean.vars <- grep( pattern=mean.pattern,x=not_counts,
                      perl=TRUE,value=TRUE)
    occ.vars <-  grep( pattern=occ.pattern,x=not_counts ,
                      perl=TRUE,value=TRUE)

    M <- 10000000000  #arbitrary bignum

    ic.names <- names(df)

    pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]
    max.vals <- apply( df[,count.vars], 2, max ,na.rm=TRUE)
    pos.bds <- cbind(pos.count,0,1.10*max.vals)
    ## limit the mean vars less, but exclude occ vars
    means_not_occ <- setdiff(mean.vars,occ.vars)
    if(length(means_not_occ)>0){
        pos.count <- (1:length(ic.names))[is.element(ic.names,means_not_occ)]
        pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))
    }
    ## now limit occupancy to (0,1)
    pos.count <- (1:length(ic.names))[is.element(ic.names, occ.vars)]
    pos.bds <- rbind(pos.bds,cbind(pos.count,0,1))

    print(ic.names)
    print("bounds:")
    print(pos.bds)


    exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))
    exclude.as.id.vars <- union(exclude.as.id.vars,'vds_id')

    list(exclude.as.id.vars=exclude.as.id.vars,
         pos.count=pos.count,
         mean.vars=mean.vars,
         count.vars=count.vars,
         pos.bds=pos.bds
         )


}
