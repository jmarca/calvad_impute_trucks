config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','wim','impute','calls')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
image_path <- paste(path,'images',parts,sep='/',collapse='/')
print(image_path)
dir.create(image_path,recursive=TRUE)
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
                                           maxiter=200,
                                           trackingdb=parts)

        saved_chain_lengths <-
            rcouchutils::couch.check.state(year=year,id=cdb_wimid,
                                           state='vo_imputation_chain_lengths',
                                           db=parts)
        saved_max_iterations <-
            rcouchutils::couch.check.state(year=year,id=cdb_wimid,
                                           state='vo_imputation_max_iterations',
                                           db=parts)
        testthat::expect_more_than(object=mean(saved_chain_lengths),expected=30)
        testthat::expect_less_than(object=mean(saved_chain_lengths),expected=80)
        testthat::expect_equal(object=saved_max_iterations,expected=0)

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

        ## now the post imputation stuff...plots and CSV dump
        result <- post_impute_handling(df.voimputed.agg,
                                       site_no,site_dir,year,
                                       plot_path=image_path,
                                       csv_path=path,
                                       trackingdb=parts)

        expect_that(result,equals(
                               paste(path,
                                     paste('site_no.',
                                           cdb_wimid,
                                           '.vo.imputed.2012.csv',
                                           sep=''),
                                     sep='/')
                           ))
        ## expect that the five post-imputation plots are in couchdb
        doc <- rcouchutils::couch.get(parts,cdb_wimid)
        attachments <- doc[['_attachments']]
        testthat::expect_that(attachments,testthat::is_a('list'))
        testthat::expect_that(sort(names(attachments)),
                              testthat::equals(
                                  c(paste(cdb_wimid,year,
                                          'imputed_vo',
                                          c("001.png",
                                            "002.png",
                                            "003.png",
                                            "004.png"),
                                          sep='_')
                                    ))
                              )


    })




unlink(paste(path,
             paste('site_no.',
                   cdb_wimid,
                   '.vo.imputed.2012.csv',
                   sep=''),
             sep='/'))
unlink(image_path <- paste(path,'images',parts[1],sep='/',collapse='/'),recursive = TRUE)
rcouchutils::couch.deletedb(parts)
