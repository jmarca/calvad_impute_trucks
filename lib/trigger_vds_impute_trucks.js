/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');

var get_vds = require('./get_vds')
var trigger_R_job = require('./trigger_R_job')

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
                // trim for testing
                detector_list = detector_list.slice(0,10)
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
