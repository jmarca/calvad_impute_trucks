/* global require console process describe it */

var should = require('should')

var trigger = require('../lib/trigger_R_job')
var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = path.normalize(rootdir+'/../test.config.json')
var config={}
var config_okay = require('config_okay')

var fs = require('fs')

describe('call to R',function(){
    var config
    after(function(done){
        fs.unlink('log/vds_1313131_2010.log',done)
    })
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

        trigger({'year':2010
                ,'config':config
                ,'funky':'music'
                ,'vdsid':1313131
                ,'opts':{cwd: Rhome
                        ,env: process.env
                        }
                }
               ,function(e,r){
                    should.not.exist(e)
                    should.not.exist(r)
                    fs.readFile('log/vds_1313131_2010.log'
                               ,{encoding:'utf8'}
                               ,function(e,data){
                                    if (e) throw e
                                    data.should.eql("[1] \"okay\"\n")
                                    return done()
                                })

                })
    })
})
