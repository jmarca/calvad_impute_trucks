config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','wim','impute','calls')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
year <- 2012
maxiter <- 200

force.plot <- TRUE

file <- paste(path,'wim.64.W.vdsid.400071.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=400071,
                      file=file)

file <- paste(path,'wim.12.S.vdsid.767366.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767366,
                      file=file)

file <- paste(path,'wim.13.N.vdsid.767367.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767367,
                      file=file)

site_no <- 81
site_dir <- 'S'
cdb_wimid <- paste('wim',site_no,site_dir,sep='.')

res <- rcouchutils::couch.put.view(parts,'vds',paste(path,'vds.json',sep='/'))
res <- rcouchutils::couch.put.view(parts,'wim',paste(path,'wim.json',sep='/'))

test_that(
    "can impute trucks files",{
        wim_pairs <- list()
        wim_pairs[[1]] <- list(vds_id=400071,wim_site=64,direction='W')
        wim_pairs[[1]] <- list(vds_id=767366,wim_site=12,direction='S')
        wim_pairs[[1]] <- list(vds_id=767367,wim_site=13,direction='N')

        df.voimputed.agg <- impute.wim.n.o(site_no = site_no,
                                           wim_dir=site_dir,
                                           wim_pairs=wim_pairs,
                                           year=year,
                                           wim_path=path,
                                           output_path=path,
                                           maxiter=200,
                                           trackingdb=parts)



        ## content checks
        df.wim.imputed <-
            calvadrscripts::get.amelia.wim.file.local(site_no=site_no
                                                     ,year=year
                                                     ,direction=site_dir
                                                     ,path=path)
        df.wim.imputed <- calvadrscripts::wim.medianed.aggregate.df(df.wim.imputed)
        df.wim.imputed[,'vds_id'] <- cdb_wimid
        ## not used
        df.wim.imputed[,'obs_count'] <- NULL

        ## there should be no diff between pre and post imputation for WIM data
        volocc_vars <- grep('^(o|n)(r|l)\\d',x=names(df.wim.imputed),perl=TRUE,value=TRUE)

        for(column in setdiff(names(df.wim.imputed),volocc_vars)){
            expect_that(length(df.wim.imputed[,column]),
                        equals(length(df.voimputed.agg[,column])))
            expect_that(summary(df.wim.imputed[,column]),
                        equals(summary(df.voimputed.agg[,column])))
        }

    })




## unlink(paste(path,'/vds_id.',vds_id,'.truck.imputed.',year,'.',c(1,2,3),'.csv',sep=''))
## unlink(paste(path,'/vds_id.',vds_id,'.truck.imputed.',year,'.csv',sep=''))
## unlink(paste(path,'/images'),recursive = TRUE)
## rcouchutils::couch.deletedb(parts)
