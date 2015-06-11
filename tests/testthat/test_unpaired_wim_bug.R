## calling impute with options 317517 list(vds_id = NaN, wim_site =
## 49, direction = \"N\") list(vds_id = NaN, wim_site = 49, direction
## = \"S\") list(vds_id = 317619, wim_site = 72, direction = \"E\")
## list(vds_id = 317624, wim_site = 72, direction = \"W\") 2012
## /data/backup/pems/breakup/ /data/backup/pems/imputed/ 200
## vdsdata%2ftracking

## the problems are two-fold.  first, why isn't site 49 paired if it
## is closest to this VDS site.  Second, the fetching code should not
## crash when passed nonsense data (NaN for VDS id)

## fixed upstream
