var fs = require('fs');
var spawn = require('child_process').spawn
var path    = require('path')
var env = process.env
if(!env.CALVAD_VDSWIM_IMPUTE_MAXITER) env.CALVAD_VDSWIM_IMPUTE_MAXITER = 200


function trigger_R_job(vds_id,wim_sites,task,done){

    var RCall = ['--no-restore','--no-save',task.config.R.script]
    var year = task.year
    var Ropts ={env:{}}
    Object.keys(env).forEach(function(k){
        Ropts.env[k]=env[k]
        return null
    })

    Ropts.env['CALVAD_YEAR']=year
    Ropts.env['CALVAD_VDS_ID']=vds_id

    // pass the list of neighbor WIM-VDS pairs
    var wim_pairs_string=[]
    // pick off the closest WIM sites.

    var mindist = wim_sites[0].dist + 1000 // some buffer, 1km
    wim_sites.forEach(function(v){
        if(v.dist < mindist){
            console.log(v)
            var direction = v.direction.substring(0,1).toUpperCase()
            var cdb_wimid = 'wim.'+v.site_no+'.'+direction
            var paired_vds = task.wim_site_pairs[cdb_wimid].merged
            if(Array.isArray(paired_vds)){
                paired_vds.forEach(function(vds){
                    wim_pairs_string.push(+vds,v.site_no,direction)
                    return null
                })
            }else{
                wim_pairs_string.push(+paired_vds,v.site_no,direction)
            }
        }
        return null
    })
    Ropts.env['CALVAD_WIM_PAIRS']=wim_pairs_string.join(',')

    if(task.message){
        console.log(task.message)
    }else{
        console.log('spawn job for ',vds_id,year)
    }
    var R  = spawn('/usr/bin/Rscript', RCall, Ropts);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = ['log/vds',vds_id,year].join('_')+'.log'
    var logstream = fs.createWriteStream(logfile
                                        ,{flags: 'a'
                                         ,encoding: 'utf8'
                                         ,mode: 0666 })
    var errstream = fs.createWriteStream(logfile
                                        ,{flags: 'a'
                                         ,encoding: 'utf8'
                                         ,mode: 0666 })
    R.stdout.pipe(logstream)
    R.stderr.pipe(errstream)
    R.on('exit',function(code){
        console.log(['got exit: ',code, 'for',vds_id,year].join(' '))
        // debug
        //throw new Error('croak while testing')
        return done()
    })
}

module.exports=trigger_R_job
