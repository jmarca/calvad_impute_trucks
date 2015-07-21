config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','csv','dump')
rcouchutils::couch.makedb(parts)

path <- './files'
file <- paste(path,'wim.12.S.vdsid.767366.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767366,
                      file=file)
file <- paste(path,'wim.13.N.vdsid.767367.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767367,
                      file=file)

res <- rcouchutils::couch.put.view(parts,'vds','./files/vds.json')
res <- rcouchutils::couch.put.view(parts,'wim','./files/wim.json')

year <- 2012
output_path <- './files/csv'
dir.create(output_path)

test_that(
    "will dump the paired WIM-VDS from couchdb",{

        wim_site <- 12
        wim_dir <- 'S'
        year <- 2012
        res <- dump.wim.csv(wim_site,wim_dir,year,
                            wim_path=path,
                            output_path=output_path,
                            trackingdb = parts)
        expect_that(res,equals('./files/csv/wim.12.S.truck.imputed.2012.csv'))
    })

test_that(
    "will crash when trying to dump the imputed WIM if no pair in couchdb",{
        wim_site <- 37
        wim_dir <- 'S'
        year <- 2012
        expect_error(
            dump.wim.csv(wim_site,wim_dir,year,
                         wim_path=path,
                         output_path=output_path,
                         trackingdb = parts)
        )

    })


unlink(output_path,recursive=TRUE)

## rcouchutils::couch.deletedb(parts)
