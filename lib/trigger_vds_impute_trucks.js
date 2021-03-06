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
var create_dist_table = require('./create_vds_wim_dist_table.js')

// async flow control stuff
var queue = require('queue-async')
var num_CPUs = require('os').cpus().length;
// testing
//num_CPUs = 1

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

var rscript
if(argv.rscript === undefined){
    rscript = 'vdswim_impute.R'
}else{
    rscript = argv.rscript
}
console.log('setting R script file that will be invoked to ',rscript,'.  Change with the -rscript option or the config file under R:vds_script')

var config
var config_okay = require('config_okay')
var doover = false
var years

/**
 * parse configuration parameters
 *
 * The config.json parameters should contain postgresql and couchdb
 * connection information.  In addition, this program is going to look
 * for
 *
 * {calvad:{years: array or a single number,
 *         ,truckimpute_redo: truthy means redo all truck imputation runs
 *         ,start_vdsid: whether redo or not, the first vdsid to start
 *            on.  Use zero to do them all; use 1200000 to start with d12
 *            as well as any high-valued D05 detectors, etc.  Interpreted
 *            as a numeric value for sorting purposes
 *  }}
 *
 * @param {} cb
 *
 * @returns {null} nothing at all, but will call the callback with
 * null,config object if success in reading the config file
 */
function _configure(cb){
    if(config === undefined){
        config_okay(config_file,function(e,c){
            config = c
            if(config.calvad !== undefined){
                // override the above hard coding stuffs
                if(config.calvad.years !== undefined){
                    if(!Array.isArray(config.calvad.years)){
                        config.calvad.years = [config.calvad.years]
                    }
                    years = config.calvad.years
                }
                if(config.calvad.truckimpute_redo !== undefined){
                    doover =config.calvad.truckimpute_redo
                }
            }

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


function year_handler(year,cb){

    var Ropt =_.extend({'year':year
                        ,'cwd': Rhome
                        ,'env': process.env
                       },config)
    var minimum_id = +config.calvad.start_vdsid

    var couch_opt =_.extend({'year':year
                            },config)

    if(config.R.vds_script === undefined){
        // need to set the right R script here, faked in the config file
        Ropt.config = {R:{vds_script:rscript}}
    }

    // get this year's valid WIM and VDS, and then set up the temp
    // table to look up distances using only those two sets of detectors.
    // valid means couchdb says imputation finished.
    // so I need a view that says as such
    var seQ = queue()
    seQ.defer(vds_imputed, couch_opt)
    seQ.defer(wim_imputed, couch_opt)
    // if requested a redo, then do all of them, otherwise
    // just do the "todo" sites
    if(!doover){
        seQ.defer(vds_imputed.get_vds_truckimputed_todo, couch_opt)
    }
    seQ.await(function(e,vds_sites,wim_sites,vds_todo_sites){
        Ropt.vds_sites = vds_sites
        Ropt.wim_sites = wim_sites

        // if requested a redo, then do all of them, otherwise
        // just do the "todo" sites
        if(doover ||
           vds_todo_sites === undefined ||
           vds_todo_sites.length == 0
          ){
              // hack.  filter out rows, because when I'm redoing ugh, don't
              // want to have to redo all of them over and over again.
              vds_todo_sites = vds_sites.filter(function(row){
                  return +row.id >= minimum_id
              })

          }
        Ropt.vds_todo_sites = vds_todo_sites

        Ropt.config = config
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

mainQ.defer(_configure)
mainQ.await(function(e){
    if (years === undefined){
        throw new Error('please define year or years in the config.json file, as for example: {..., calvad:{years: [2012], ...}, ... }')
    }

    years.forEach(function(year){
        yearQ.defer(year_handler,year)
        return null
    })

    yearQ.await(function(e,r){
        if(e) throw new Error(e)
        processQ.await(function(ee,rr){
            console.log('wim queue drained for all years')
            return null
        })
    })
    return null
})
