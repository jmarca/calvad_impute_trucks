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

function process_years(config){

    yearQ = queue()
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

                // var vds_ids = vds_sites.map(function(doc){
                //     return doc.id
                // })

                create_dist_table(Ropt,function(e,r){
                    // okay distance table is created in the current psql db conn
                    Object.keys(r).forEach(function(vds_id){

                        // if( vds_ids.indexOf(vds_id) === -1 ){
                        //     console.log('bailing on vds id ',vds_id,', not a real site for this year')
                        //     return null
                        // }

                        //
                        // got a matrix of distances between WIM, VDS
                        //
                        // process each VDS with its nearest WIM sites
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
}



// old code way
function run(cb){
    // check config
    _configure(function(e,config){
        var years = config.years
        if (years === undefined){
            years = [2010]
        }
        // get vds sites that are done
        var year_q = queue(1)
        years.forEach(function(y){
            year_q.defer(get_vds,{'year':y
                                 ,'config_file':config_file
                                 })
            return null
        })
        year_q.awaitAll(function(e,results){
            // if error, die
            if(e) throw new Error(e)
            // results is a list of lists, one per year
            var R_q = queue(num_CPUs)
            years.forEach(function(y,i){
                var detector_list = results[i]
                var queue_size = detector_list.length
                detector_list.forEach(function(vds,i){
                    R_q.defer(trigger_R_job
                             ,{'year':y
                              ,'config':config
                              ,'vdsid':vds.vdsid
                              ,'opts':{'cwd':Rhome
                                      ,'env':process.env
                                      }
                              ,'message':[vds.vdsid,y,', job',i,'of',queue_size]
                                         .join(' ')
                              })
                    return null
                })
                return null
            })
            R_q.awaitAll(function(e,r){
                if(e) throw new Error(e)
                console.log('done with R jobs')
                cb(null)
            })
            return null
        })
        return null
    })
    return null
}

run(function(){console.log('all done')})
