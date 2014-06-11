/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var _ = require('lodash');
var couch_check = require('couch_check_state')

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

var get_vds = require('./get_vds')

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
// so I need list of WIM/VDS pairs
// so I need vds neighbors

// or I can just leave all that to the R code, let R decide


var file_queue=async.queue(trigger_R_job,num_CPUs)
file_queue.drain =function(){
    console.log('queue drained')
    return null
}
var unique_vds = {}
function doit(err,config){
    var years = config.years
    opts.config=config
    _.each(years,function(year){
        var c = _.clone(config)
        c.year = year
        vds_sites.get_wim_need_pairing(c
                                      ,)
                                   })
});
