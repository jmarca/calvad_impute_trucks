config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','wim','impute','calls')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
year <- 2012
maxiter <- 200

force.plot <- TRUE

file <- paste(path,'/wim.64.W.vdsid.400071.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=400071,
                      file=file)

site_no <- 81
site_dir <- 'S'
cdb_wimid <- paste('wim',site_no,site_dir,sep='.')

res <- rcouchutils::couch.put.view(parts,'vds',paste(path,'vds.json',sep='/'))
res <- rcouchutils::couch.put.view(parts,'wim',paste(path,'wim.json',sep='/'))

test_that(
    "can impute trucks files",{
    wim.pairs <- list()
    wim.pairs[[1]] <- list(vds_id=400071,wim_site=64,direction='W')
    result <- impute.wim.n.o(site_no = wim_site,
                             wim_dir=wim_dir,
                             wim_pairs=wim_pairs,
                             year=year,
                             wim_path=path,
                             output_path=path,
                             maxiter=200,
                             trackingdb=parts)
    testthat::expect_match(result,'truck.imputed.2012.csv$')
    result <- impute.vds.site(vds_id=vds_id,
                                  wim_pairs=wim_pairs,
                                  year=year,
                                  vds_path=path,
                                  output_path=path,
                                  maxiter=200,
                                  trackingdb=parts)
        ## second time through, expect that the filename has been incremented
        testthat::expect_match(result,'truck.imputed.2012.1.csv$')

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
