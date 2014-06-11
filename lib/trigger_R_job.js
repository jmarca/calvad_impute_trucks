var fs = require('fs');
var spawn = require('child_process').spawn
var path    = require('path')
var outputdir = path.normalize(__dirname+'/imputed')
var env = process.env
if(!env.CALVAD_IMPUTE_PATH) env.CALVAD_IMPUTE_PATH = outputdir
if(!env.CALVAD_LOCAL_COUCH) env.CALVAD_LOCAL_COUCH = 1
if(!env.CALVAD_VDSWIM_IMPUTE_MAXITER) env.CALVAD_VDSWIM_IMPUTE_MAXITER = 100
if(!env.CALVAD_PEMS_ROOT) env.CALVAD_PEMS_ROOT = '/data/backup/pems'


function trigger_R_job(task,done){

    var RCall = ['--no-restore','--no-save',task.config.R.script]
    var vds = task.vdsid
    var year = task.year
    if(task.env === undefined) task.env = {}
    Object.keys(env).forEach(function(k){
        task.env[k]=env[k]
        return null
    })
    task.env['CALVAD_YEAR']=year
    task.env['CALVAD_VDSID']=vds

    if(task.message) console.log(task.message)

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

module.exports=trigger_R_job