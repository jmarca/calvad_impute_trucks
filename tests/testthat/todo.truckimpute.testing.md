# QAQC in process

fixed some things

need to fix aggregation of speed...divide by count, eh?

plots work okay

BROKEn merge of lanes


# todo to setup truck imputes

1. load wim imputed data

1.1 copy wim imputed to tests/testthat/files
1.2 load a wim imputed file
1.3 check that loading, aggregating works okay

2. pair vds wim

1.1. copy vds data
1.2. copy wim data
1.3. load vds imputed
1.4. load wim imputed
1.5. merge vds wim
1.6. load another wim
1.7. merge the other wim
1.8. combine

do or do not combine aggregate imputations?



1. test case site 1108541.  get data into file system

2. wim sites 37 and 87.  Load data into couchdb docs, ready to loadup


```
spatialvds=# select * from imputed.vds_wim_pairs where wim_id in (37,87);
 id  | vds_id  | wim_id | freeway | direction |     distance
-----+---------+--------+---------+-----------+------------------
  59 | 1114507 |     87 |      15 | south     | 112.301327592088
  84 | 1114696 |     37 |      15 | south     | 61212.8823441913
  86 |  808808 |     37 |      15 | south     | 26487.5340506519
 180 |  817762 |     37 |      15 | south     | 766.857938033151
 195 | 1100595 |     87 |      15 | south     | 112.301327592088
 246 | 1115802 |     87 |      15 | south     | 278.884135308249
```
3. wim pairing data

4. wim pairing code first, to load 37, 87

5. wim imputed retrieval code needed.

6. where am I storing the imputed wim results?  in process.wim.site.R,
   I save to "target.file" that is the output from
   make.amelia.output.file with parameters savepath, and wim, site.id,
   direction, seconds, and year.  So in the file system, not in couchdb


7. need tests for wim impute code too, eh?
