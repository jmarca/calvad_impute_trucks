/* global require console process describe it */

var should = require('should')

var trigger = require('../lib/trigger_R_job')
var trigger2 = require('../lib/trigger_R_wimwim_job.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = path.normalize(rootdir+'/../test.config.json')
var config={}
var config_okay = require('config_okay')

var fs = require('fs')

describe('call to R v1',function(){
    var config
    before(function(done){
        config_okay(config_file,function(err,c){
            if(!c.couchdb.db){ throw new Error('need valid db defined in test.config.json')}
            config = c

            return done()
        })
        return null
    })
    it('should call R and not choke, and env vars okay',function(done){
        var rootdir = path.normalize(__dirname)
        var Rhome = path.normalize(rootdir)
        console.log(Rhome)

        trigger(1313131,[1,2,3,4],
                {'year':2010
                 ,'config':config
                 ,'funky':'music'
                 ,'vdsid':1313131
                }
               ,function(e,r){
                    should.not.exist(e)
                    should.not.exist(r)
                    fs.readFile('log/vds_1313131_2010.log'
                               ,{encoding:'utf8'}
                               ,function(e,data){
                                    if (e) throw e
                                   data.should.eql("[1] \"okay\"\n")
                                   fs.unlink('log/vds_1313131_2010.log',
                                             done)
                                })

                })
    })
    it('should call R and not choke v2, and env vars okay',function(done){
        var rootdir = path.normalize(__dirname)
        var Rhome = path.normalize(rootdir)
        console.log(Rhome)

        trigger2(13,'N',
                 {'year':2010
                  ,'config':config
                  ,'funky':'music'
                  ,'site_no':13
                  ,'site_dir':'N'
                  //,'cwd': Rhome
                  ,'env': process.env
                 }
                 ,function(e,r){
                     should.not.exist(e)
                     should.not.exist(r)
                     fs.readFile('log/wimwim_13_N_2010.log'
                                 ,{encoding:'utf8'}
                                 ,function(e,data){
                                     if (e) throw e
                                     data.should.eql("[1] \"okay\"\n")
                                     fs.unlink('log/wimwim_13_N_2010.log',
                                               done)
                                 })

                 })
    })
})
