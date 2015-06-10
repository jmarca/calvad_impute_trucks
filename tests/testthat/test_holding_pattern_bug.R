config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('holding','pattern','bug')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
year <- 2012
maxiter <- 200

force.plot <- TRUE

file <- paste(path,'wim.12.S.vdsid.767366.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767366,
                      file=file)
file <- paste(path,'wim.13.N.vdsid.767367.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767367,
                      file=file)

vds_id <- 774031

test_that(
    "will not hit the holding pattern bug",{
        wim_pairs <- list()
        wim_pairs[[1]] <- list(vds_id=767366,wim_site=12,direction='S')
        wim_pairs[[2]] <- list(vds_id=767367,wim_site=13,direction='N')
        result <- impute.vds.site(vds_id=vds_id,
                                  wim_pairs=wim_pairs,
                                  year=year,
                                  vds_path=path,
                                  output_path=path,
                                  maxiter=maxiter,
                                  trackingdb=parts)
        testthat::expect_match(result,'truck.imputed.2012.csv$')


    })


unlink(paste(path,'/vds_id.',vds_id,'.truck.imputed.',year,'.csv',sep=''))
unlink(paste(path,'/images/',vds_id,sep=''),recursive = TRUE)
rcouchutils::couch.deletedb(parts)
