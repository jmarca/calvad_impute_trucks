# CalVAD impute trucks

This repository contains code to impute trucks at VDS loop detector
sites in California, using the output of other CalVAD processes.

# Prerequisites

The code requires that other steps have been taken.  These are:

1. Each VDS site has already been "back-filled"---all of its missing
   data has been imputed, using the code in `calvad_impute_missing`.

2. Each VDS detector site has been associated with one or more WIM
   sites, in order to have observed data with which to impute the
   estimates of truck traffic.

3. Each WIM site used in step 2 must already have been *paired* with
   their nearest VDS station using the `calvad_merge_pairs` code.

If a VDS site has no data, very little data, or extremely poor quality
data, you won't be able to impute truck estimates.

If a VDS site is associated (in step 2) with a WIM station that has no
data or very little data, you won't be able to impute truck estimates.

Furthermore, the code depends upon a view in CouchDB that generates a
listing of VDS sites that still need processing.

# Running the code

To run the code, set up the appropriate configurations in
`config.json`, and make sure that the file is private by running

```
chmod 0600 config.json
```

If you don't do that, then `config_okay` will refuse to read the
file.  (It is a bad idea to store passwords in a file that anybody can
read.)

Once that is set, you should be able to run the imputations by running

```
node ./lib/trigger_vds_impute_trucks.js
```

and so on.


# Overview of what is here, what it does, what is deprecated

The purpose of this repository is to impute missing WIM-related variables at a
VDS site, and to impute missing VDS-related variables at WIM sites.
As noted in the Prerequisites section above, both of the imputation
runs depend upon a set of paired VDS-WIM sites.  If a VDS and a WIM
site are nearby in space and measuring the same freeway and direction
of travel, then they can be paired together, imitating a single
complete observation.  This can be used at other sites by Amelia to
impute the most likely "missing" values.

In order to run the VDS imputation of variables like heavy heavy-duty
truck counts, one should run the command

```
node ./lib/trigger_vds_impute_trucks.js
```

In order to run the WIM imputation of variables like volume and
occupancy, one should run the command

```
node ./lib/trigger_wim_impute_v_o.js
```

## `trigger_vds_impute_trucks.js`

This function takes a few command line options and by default reads configuration
details from the file `config.json`.  The command line options it
reads are:

* --config defaults to config.json.  Use this option to specify an
  alternate configuration file.  Example  --config=my/special/config.json

* --rscript defaults to vdswim_impute.R.  Use this option to specify
  an alternate file to set up and run the imputation step.  Primary
  purpose if for use by the tests to run a dummy imputation routine.

The configuration file config.json must be set to read-only mode
(chmod 0600).  It should contain specifications for how to connect to
PostgreSQL and CouchDB.  It should also contain details about what
this imputation step should be doing in the field called "calvad".  An
example config file is:

```json
{
    "couchdb": {
        "host": "127.0.0.1",
        "port":5984,
        "trackingdb":"vdsdata%2ftracking",
        "db":"vdsdata%2ftracking",
        "auth":{"username":"cdb username",
                "password":"my super secret hard to guess password"
               }
    },
    "postgresql": {
        "host": "127.0.0.1",
        "port":5432,
        "db": "spatialvds",
        "auth":{"username":"psql username",
                "password":"optional password here, will use .pgpass file"
               }
    },
    "calvad":{
        "years" : [2012],
        "truckimpute_redo": false,
        "start_vdsid": 0,
        "wimpath":"./tests/testthat/files/",
        "vdspath":"./tests/testthat/files/"
    }
}
```

The important variables for the imputation process are stored in the
`calvad` entry of the configuration file.

* years:  what years to process.  Only one year at a time is well
  tested.

* truckimpute_redo:  true or false.  If false, CouchDB tracking
  database will be checked to determine if a VDS site has already done
  its truck imputation.  If true, then CouchDB will not be checked,
  and all VDS sites with valid raw imputation will be run through the
  truck imputation step.

* start_vdsid:  The starting VDS id for the imputation.  This is
  useful if for example, you want to redo imputations, but need to
  stop and restart the job for some reason.  Note that this is a
  number, even though the first digit or digits can be interpreted as
  the Caltrans district.  If you set the number as 120000, then all of
  District 12 will be re-run, as well as the detectors in District 5
  that have very high detector numbers (for some reason, Caltrans
  decided to make District 5 detectors very long).  Set to zero on a
  new run.

* wimpath:  where in the file system are the WIM data imputations
  stored.

* vdspath:  where in the file system are the VDS data imputations
  stored.

All other entries in this part of the configuration file are ignored.
Thus it is possible to comment out older options by changing their
names.  For example, to track completed runs, you might have:

```
{
     ...
    "calvad":{
        "years" : [2012],
        "finished_years" : [2007, 2008, 2009, 2010, 2011],
        "truckimpute_redo": false,
        "start_vdsid": 0,
        "wimpath":"./tests/testthat/files/",
        "vdspath":"./tests/testthat/files/",
        "vdspath_2007":"2007/data/went/here/"
    }
}
```

## `trigger_wim_impute_v_o.js`

This function takes a few command line options and by default reads configuration
details from the file `config.json`.  The command line options it
reads are:

* --config defaults to config.json.  Use this option to specify an
  alternate configuration file.  Example  --config=my/special/config.json

* --rscript defaults to wimvds_impute.R.  Use this option to specify
  an alternate file to set up and run the imputation step.  Primary
  purpose if for use by the tests to run a dummy imputation routine.

The configuration file is exactly the same as that used for
`trigger_vds_impute_trucks.js`.


The differences are as follows.  Because there are only a hundred or
so WIM stations, there is no need for either a "redo" option or a
starting number.  Every WIM site is rerun with every job.  Thus
options like "start_wimsite" and "wim_vo_impute_redo" do not exist.
