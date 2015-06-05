var fs = require('fs');
var spawn = require('child_process').spawn
var path    = require('path')
var outputdir = path.normalize(__dirname+'/imputed')
var env = process.env
if(!env.CALVAD_OUTPUT_PATH) env.CALVAD_IMPUTE_PATH = outputdir
if(!env.CALVAD_VDSWIM_IMPUTE_MAXITER) env.CALVAD_VDSWIM_IMPUTE_MAXITER = 200
if(!env.CALVAD_VDS_PATH) env.CALVAD_PEMS_ROOT = '/data/backup/pems'


function trigger_R_job(vds_id,wim_sites,task,done){

    var RCall = ['--no-restore','--no-save',task.config.R.script]
    var year = task.year
    var Ropts ={}
    Object.keys(env).forEach(function(k){
        Ropts.env[k]=env[k]
        return null
    })
    if(task.opts.env !== undefined){
        Object.keys(task.opts.env).forEach(function(k){
            Ropts.env[k] = task.opts.env[k]
            return null
        })
    }

    Ropts.env['CALVAD_YEAR']=year
    Ropts.env['CALVAD_VDS_ID']=vds_id

    // pass the list of neighbor WIM-VDS pairs
    var wim_pairs_string=[]
    Object.keys(wim_sites).forEach(function(k){
        wim_pairs_string.push(wim_sites[k])
        return null
    })
    Ropts.env['CALVAD_WIM_PAIRS']=wim_pairs_string.join(',')

    if(task.message) console.log(task.message)

    var R  = spawn('Rscript', RCall, task.opts);
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
        // debug
        throw new Error('croak while testing')
        return done()
    })
}

module.exports=trigger_R_job
