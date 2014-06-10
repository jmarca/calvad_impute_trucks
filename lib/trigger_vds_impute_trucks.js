/*global console */
var util  = require('util'),
    spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');
var async = require('async');
var _ = require('lodash');
var couch_check = require('couch_check_state')
var vds_sites = require('calvad_vds_sites')

var num_CPUs = require('os').cpus().length;

// configuration stuff

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
// so I need list of WIM/VDS pairs
// so I need vds neighbors

// or I can just leave all that to the R code, let R decide


var R;

var trigger_R_job = function(task,done){
    var RCall = ['--no-restore','--no-save',task.config.R.script]
    var vds = task.vds
    var year = task.year
    task.env['RYEAR']=year
    task.env['VDS_SITE']=vds

    var R  = spawn('Rscript', RCall, task);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = ['log/vds',vds,year].join('_')+'.log'
    var logstream = fs.createWriteStream(logfile
                                        ,{flags: 'a'
                                         ,encoding: 'utf8'
                                         ,mode: 0666 })
    R.stdout.pipe(logstream)
    R.stderr.pipe(logstream)
    R.on('exit',function(code){
        console.log(['got exit: ',code, 'for',vds,year].join(' '))
        // throw new Error('die')
        return done()
    })
}
var file_queue=async.queue(trigger_R_job,num_CPUs)
file_queue.drain =function(){
    console.log('queue drained')
    return null
}
var unique_vds = {}
var rootdir = path.normalize(__dirname)
var Rhome = path.normalize(rootdir+'/../R')
var opts = {cwd: Rhome
           ,env: process.env
           }
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
