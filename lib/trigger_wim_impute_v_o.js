/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var _ = require('lodash')

var argv = require('minimist')(process.argv.slice(2))

var trigger_R_job = require('./trigger_R_wimwim_job')

var wim_imputed = require('./wim_imputed.js')
var create_dist_table = require('./create_wim_wim_dist_table.js')

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
    rscript = 'wimvds_impute.R'
}else{
    rscript = argv.rscript
}
console.log('setting R script file that will be invoked to ',rscript,'.  Change with the -rscript option or the config file under R:wim_script')

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

// for debugging runs
// var defer_one = 1

function year_handler(year,config,cb){

    var Ropt =_.extend({'year':year
                        //,'cwd': Rhome
                        ,'env': process.env
                       },config)

    if(config.R.wim_script === undefined){
        // set the right R script here, rather than in the config file
        Ropt.config = {R:{wim_script:rscript}}
    }
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
        config.wim_sites = wim_sites

        create_dist_table(config,function(e,table){
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

            // get non-paired sites from distance table keys

            // make a lookup of wim site data
            var wim_site_docs = {}
            wim_sites.forEach(function(resp){
                var doc = resp.doc
                wim_site_docs[resp.id]=doc[year]
                var property_key = Object.keys(doc.properties).sort().pop()
                wim_site_docs[resp.id].properties=doc.properties[property_key]
            })

            // iterate over the non-paired sites...the keys from the
            // distance matrix
            _.each(table,function(wim_dists,cdb_wimid){
                // first, skip this site if imputed !== 'finished'

                if(wim_site_docs[cdb_wimid].imputed !== 'finished'){
                    console.log('imputed not finished, but',
                                wim_site_docs[cdb_wimid].imputed)
                    return null
                }

                // okay, now skip if vo_imputation_max_iterations is
                // defined and zero
                // if(wim_site_docs[cdb_wimid].vo_imputation_max_iterations !== undefined &&
                //    wim_site_docs[cdb_wimid].vo_imputation_max_iterations === 0){
                //     console.log('wim site',cdb_wimid,'already done')
                //     return null
                // }

                // still here, then the non-paired is properly
                // imputed, time to impute most likely vol, occ.

                // pair only with equal or less numbers of lanes
                var wim_equal_lanes=[]
                var wim_fewer_lanes=[]

                var mylanes = wim_site_docs[cdb_wimid].properties.lanes
                if(mylanes < 2) mylanes = 2 // probably not an issue, but still
                var mysite = wim_dists[0]
                        .notpaired_site_no
                var mydir = wim_dists[0]
                        .notpaired_direction
                        .substr(0,1)
                        .toUpperCase()
                wim_dists.forEach(function(row){
                    var pwim = row.paired_site_no
                    var pdir = row.paired_direction.substr(0,1)
                            .toUpperCase()
                    var paired_cdbwimid = 'wim.'+pwim+'.'+pdir

                    var other_lanes =
                            wim_site_docs[paired_cdbwimid].properties.lanes
                    var paired_vds = wim_site_docs[paired_cdbwimid].merged
                    if(mylanes == other_lanes) {
                        // push to equal lanes
                        if(Array.isArray(paired_vds)){
                            paired_vds.forEach(function(vds){
                                wim_equal_lanes.push(+vds,pwim,pdir)
                                return null
                            })
                        }else{
                            wim_equal_lanes.push(+paired_vds,pwim,pdir)
                        }
                    }else{
                        // if(mylanes > other_lanes){
                        // push to fewer lanes
                        if(Array.isArray(paired_vds)){
                            paired_vds.forEach(function(vds){
                                wim_fewer_lanes.push(+vds,pwim,pdir)
                                return null
                            })
                        }else{
                            wim_fewer_lanes.push(+paired_vds,pwim,pdir)
                        }
                    }
                    return null
                })

                // merge equal, then fewer...thus sorting by number of
                // lanes first, distance second.
                var wim_pairs = wim_equal_lanes.concat(wim_fewer_lanes)

                // just keep at most 3
                if(wim_pairs.length > 3){
                    wim_pairs = wim_pairs.slice(0,3*3) // keep 0,1,2
                }

                Ropt.env['CALVAD_WIM_PAIRS'] = wim_pairs.join(',')

                //
                // r(vds_id) is a list of wim sites and
                // distances.  Pass to R to process each VDS
                // with its nearest WIM sites
                //
                processQ.defer(trigger_R_job,
                               mysite,mydir,
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
