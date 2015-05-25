/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');

//var get_vds = require('./get_vds')
var trigger_R_job = require('./trigger_R_job')

var vds_imputed = require('../lib/vds_imputed.js')
var wim_imputed = require('../lib/wim_imputed.js')
var create_dist_table = require('../lib/create_dist_table.js')

// async flow control stuff
var queue = require('queue-async')
var num_CPUs = require('os').cpus().length;

// configuration stuff
var rootdir = path.normalize(__dirname)
var Rhome = path.normalize(rootdir+'/../R')
var opts = {cwd: Rhome
           ,env: process.env
           }

var config_file = path.normalize(rootdir+'/../config.json')
var config
var config_okay = require('config_okay')

function _configure(cb){
    if(config === undefined){
        config_okay(config_file,function(e,c){
            config = c
            return cb(null,config)
        })
        return null
    }else{
        return cb(null,config)
    }
}

// list based view of what happens

// get a list of vds sites with completed imputation

// filter that list by
// keeping only those with at least one neighboring WIM/VDS pair that is ready to go

// so I need list of VDS
//
// leave it to the R code to get
//    list of WIM/VDS pairs
//    list of vds neighbors


var yearQ = queue()
var processQ = queue(num_CPUs)

var years = config.years
if (years === undefined){
    years = [2012]
}

years.forEach(function(year){
    yearQ.defer(function(cb){

        var Ropt =_.extend({'year':year
                            ,'cwd': Rhome
                            ,'env': process.env
                           },config)
        var couch_opt =_.extend({'year':year
                                },config)

        // get this year's valid WIM and VDS, and then set up the temp
        // table to look up distances using only those two sets of detectors.
        // valid means couchdb says imputation finished.
        // so I need a view that says as such
        var seQ = queue()
        seQ.defer(vds_imputed, couch_opt)
        seQ.defer(wim_imputed, couch_opt)

        seQ.await(function(e,vds_sites,wim_sites){
            Ropt.vds_sites = vds_sites
            Ropt.wim_sites = wim_sites

            create_dist_table(Ropt,function(e,r){
                // okay distance table is created in the current psql db conn
                // the table is keyed on vdsid
                //
                // each vdsid branch in the hashtable has WIM and
                // distance most likely you'll only match to the
                // closest (the first one) but the R code might
                // want more that the first one.
                //

                Object.keys(r).forEach(function(vds_id){
                    //
                    // r(vds_id) is a list of wim sites and
                    // distances.  Pass to R to process each VDS
                    // with its nearest WIM sites
                    //
                    processQ.defer(trigger_R_job,
                                   vds_id,
                                   r[vds_id],
                                   Ropt)

                    return null
                })
                return cb() // done with year's deferred handler,
                // wimQ loaded for this year

            })
            return null
        })
        return null

    })
    return null


})
yearQ.await(function(e,r){
    if(e) throw new Error(e)
    processQ.await(function(ee,rr){
        console.log('wim queue drained for all years')
        return null
    })
})
