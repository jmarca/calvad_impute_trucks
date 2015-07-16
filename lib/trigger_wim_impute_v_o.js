/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var _ = require('lodash')

var argv = require('minimist')(process.argv.slice(2))

//var get_vds = require('./get_vds')
var trigger_R_job = require('./trigger_R_job')

var vds_imputed = require('./vds_imputed.js')
var wim_imputed = require('./wim_merged.js')
var create_dist_table = require('./create_wim_wim_dist_table.js')

// async flow control stuff
var queue = require('queue-async')
var num_CPUs = require('os').cpus().length;
// testing
num_CPUs = 1

// configuration stuff
var rootdir = path.normalize(__dirname)
var Rhome = path.normalize(rootdir+'/../R')
var opts = {cwd: Rhome
           ,env: process.env
           }

var config_file
if(argv.config === undefined){
    config_file = path.normalize(rootdir+'/../config.json')
}else{
    config_file = argv.config
}
console.log('setting configuration file to ',config_file,'.  Change with the -config option.')

var config
var config_okay = require('config_okay')


// list based view of what happens

// get a lists of imputed wim sites, and wim paired sites from couchdb
//
// generate distance matrix
//
// pair off closest combinations of unmerged and merged WIM sites with
// same or similar numbers of lanes.  Use two merged sites if possible
// to add richness to the data set.
//


function year_handler(year,config,cb){

    var Ropt =_.extend({'year':year
                        ,'cwd': Rhome
                        ,'env': process.env
                       },config)
    // set the right R script here, rather than in the config file
    Ropt.config.R.script = rscript

    var couch_opt =_.extend({'year':year
                            },config)

    // get this year's valid WIM and VDS, and then set up the temp
    // table to look up distances using only those two sets of detectors.
    // valid means couchdb says imputation finished.
    // so I need a view that says as such
    var seQ = queue()
    seQ.defer(vds_imputed, couch_opt)
    seQ.defer(vds_imputed.get_vds_truckimputed_todo, couch_opt)
    seQ.defer(wim_imputed, couch_opt)

    seQ.await(function(e,vds_sites,vds_todo_sites,wim_sites){
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
            if(e){
                throw new Error(e)
            }

            // make a lookup of wim site data
            var wim_site_pairs = {}
            wim_sites.forEach(function(doc){
                wim_site_pairs[doc.id]=doc.doc[year]
            })
            Ropt.wim_site_pairs =wim_site_pairs

            vds_todo_sites
            //Object.keys(r)
                .forEach(function(doc){
                //
                // r(vds_id) is a list of wim sites and
                // distances.  Pass to R to process each VDS
                // with its nearest WIM sites
                //
                processQ.defer(trigger_R_job,
                               doc.id,
                               r[doc.id],
                               Ropt)

                return null
            })
            return cb() // done with year's deferred handler,
            // wimQ loaded for this year

        })
        return null
    })
    return null

}
var mainQ = queue(1)

var yearQ = queue(1)
var processQ = queue(num_CPUs)

mainQ.defer(config_okay,config_file)
mainQ.await(function(e,config){

    var years = config.years
    if (years === undefined){
        years = [2012]
    }

    years.forEach(function(year){
        yearQ.defer(year_handler,year,config)
        return null
    })

    yearQ.await(function(e,r){
        if(e) throw new Error(e)
        processQ.await(function(ee,rr){
            console.log('queue drained for all years')
            return null
        })
    })
    return null
})
