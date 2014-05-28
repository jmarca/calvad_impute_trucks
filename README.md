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
node trigger_vds_impute_trucks.js
```

If I get off my lazy behind, I might set up command line arguments
like years and such.
