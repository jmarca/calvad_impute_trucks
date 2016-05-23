/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var _ = require('lodash')

var argv = require('minimist')(process.argv.slice(2))

var trigger_R_job = require('./trigger_R_wimwim_job')

var wim_imputed = require('./wim_imputed.js')

var extract_couch_docs = require('../lib/extract_couch_docs.js')

// async flow control stuff
var queue = require('queue-async')
var num_CPUs = require('os').cpus().length;
// testing
// num_CPUs = 1

// configuration stuff
var rootdir = path.normalize(__dirname)
var Rhome = path.normalize(rootdir+'/../R')

var config_file
if(argv.config === undefined){
    config_file = path.normalize(rootdir+'/../config.json')
}else{
    config_file = argv.config
}
console.log('setting configuration file to ',config_file,'.  Change with the -config option.')

var rscript
if(argv.rscript === undefined){
    rscript = 'wimvds_pairdump.R'
}else{
    rscript = argv.rscript
}
console.log('setting R script file that will be invoked to ',rscript,'.  Change with the -rscript option.')

var config_okay = require('config_okay')

var cdb_wimid_pattern = /^wim.(\d+).([NSEW])$/;


// list based view of what happens

// get a lists of imputed wim sites
//
// pick off only the paired ones
//
// send the not-yet-dumped pairs to R for dumping to CSV
//


function year_handler(year,config,cb){

    var Ropt =_.extend({'year':year
                        //,'cwd': Rhome
                        ,'env': process.env
                       },config)
    // set the right R script here, rather than in the config file

    Ropt.config = {R:{wim_script:rscript}}

    // Ropt.R.script = rscript

    var couch_opt =_.extend({'year':year
                            },config)

    // get this year's valid WIM and VDS, and then set up the temp
    // table to look up distances using only those two sets of detectors.
    // valid means couchdb says imputation finished.
    // so I need a view that says as such
    var seQ = queue()
    seQ.defer(wim_imputed, couch_opt)

    seQ.await(function(e,wim_sites){

        // ferret out the paired from the unpaired
        var p_site_docs  = extract_couch_docs.extract_paired_wim(wim_sites)
        var psites = extract_couch_docs.extract_wim_sites(p_site_docs)

        p_site_docs.forEach(function(resp){
            var doc = resp.doc[year]
            var cdb_wimid = resp.id
            // iterate over the paired sites, send to dumper
            // okay, skip if wim_pair_dump is defined and non-zero
            if(doc.wim_pair_dump_csv !== undefined &&
               doc.wim_pair_dump_csv === 'finished'){
                console.log('wim site',cdb_wimid,'already dumped')
                return null
            }

            // still here then send to R dump csv code
            var result = cdb_wimid_pattern.exec(cdb_wimid)

            var mysite = result[1]
            var mydir = result[2]
            processQ.defer(trigger_R_job,
                               mysite,mydir,
                               Ropt)

            return null
        })
        return cb() // done with year's deferred handler,
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
