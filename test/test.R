
output.path <- Sys.getenv(c('CALVAD_IMPUTE_PATH'))[1]

localcouch = Sys.getenv(c('CALVAD_LOCAL_COUCH'))[1]
if(is.null(localcouch)){
  localcouch = FALSE
}else{
  localcouch = TRUE
}

district = Sys.getenv(c('CALVAD_DISTRICT'))[1]
if(is.null(district)){
  print('assign a district to the RDISTRICT environment variable')
  quit(10)
}

vds.id = Sys.getenv(c('CALVAD_VDSID'))[1]
if(is.null(vds.id)){
  print('assign a file to process to the FILE environment variable')
  quit(10)
}

year = as.numeric(Sys.getenv(c('CALVAD_YEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  quit(10)
}

seconds <- 3600

maxiter = Sys.getenv(c('CALVAD_VDSWIM_IMPUTE_MAXITER'))[1]
if(is.null(maxiter)){
    maxiter=200
}

pems.root = Sys.getenv(c('CALVAD_PEMS_ROOT'))[1]
if(is.null(maxiter)){
    pems.root <- '/data/backup/pems'
}

print('okay')
quit()
