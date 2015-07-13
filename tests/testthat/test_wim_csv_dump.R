config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','csv','dump')
rcouchutils::couch.makedb(parts)
path <- './files'
output_path <- './files'
year <- 2012

force.plot <- TRUE

file <- paste(path,'wim.12.S.vdsid.767366.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767366,
                      file=file)
file <- paste(path,'wim.13.N.vdsid.767367.2012.paired.RData',sep='/')
calvadmergepairs::couch.put.merged.pair(trackingdb=parts,
                      vds.id=767367,
                      file=file)

res <- couch.put.view(dbname,'vds','./files/vds.json')
res <- couch.put.view(dbname,'wim','./files/wim.json')

test_that(
    "will dump the paired WIM-VDS from couchdb",{

    })

test_that(
    "will dump the imputed WIM if no pair in couchdb",{

    })


# rcouchutils::couch.deletedb(parts)
