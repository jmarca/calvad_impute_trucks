/* global require console process describe it */

var should = require('should')
var wim_imputed = require('../lib/wim_imputed.js')
var wim_merged = require('../lib/wim_merged.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}
var config_okay = require('config_okay')
before(function(done){

    config_okay(config_file,function(err,c){
        if(err){
            throw new Error('node.js needs a good croak module')
        }
        config = c
        return done()
    })
    return null
})

describe('get finished imputed WIM sites',function(){
    it('should get the WIM sites that are finished imputed',function(done){
        config.year =2012
        wim_imputed(config
		    ,function(e,sites){
                        should.not.exist(e)
                        should.exist(sites)
                        sites.should.have.lengthOf(140)
                        sites.forEach(function(w){
                            w.should.have.property('doc')
                            w['doc'].should.have.property(config.year)
                            w['doc'][config.year].should.have.property('imputed')
                            return null
                        })
                        return done()
                    })
    })

})

describe('get merged WIM sites',function(){
    it('should get the WIM sites that are merged with vds sites',function(done){
        config.year =2012
        wim_merged(config
		    ,function(e,sites){
                        should.not.exist(e)
                        should.exist(sites)
                        sites.should.have.lengthOf(85)
                        sites.forEach(function(w){
                            w.should.have.property('doc')
                            w['doc'].should.have.property(config.year)
                            w['doc'][config.year].should.have.property('merged')
                            return null
                        })
                        return done()
                    })
    })

})
