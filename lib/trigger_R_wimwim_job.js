var fs = require('fs');
var spawn = require('child_process').spawn
var path    = require('path')
var env = process.env
if(!env.CALVAD_WIMWIM_IMPUTE_MAXITER) env.CALVAD_WIMWIM_IMPUTE_MAXITER = 200


function trigger_R_job(site_no,site_dir,Ropt,done){

    var RCall = ['--no-restore','--no-save',Ropt.config.R.wim_script]
    var year = Ropt.year

    // make sure these are set right
    Ropt.env['CALVAD_YEAR']=year
    Ropt.env['CALVAD_WIM_SITE']=site_no
    Ropt.env['CALVAD_WIM_DIR'] =site_dir

    if(Ropt.message){
        console.log(Ropt.message)
    }else{
        console.log('spawn job for ',site_no,site_dir,year)
    }

    var R  = spawn('/usr/bin/Rscript', RCall, Ropt);
    R.stderr.setEncoding('utf8')
    R.stdout.setEncoding('utf8')
    var logfile = ['log/wimwim',site_no,site_dir,year].join('_')+'.log'
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
        console.log(['got exit: ',code, 'for',site_no,site_dir,year].join(' '))
        // debug
        // throw new Error('croak while testing')
        return done()
    })
}

module.exports=trigger_R_job
