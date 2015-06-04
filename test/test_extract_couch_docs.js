/* global require console process describe it */

var should = require('should')
var wim_imputed = require('../lib/wim_imputed.js')
var vds_imputed = require('../lib/vds_imputed.js')
var extract_couch_docs = require('../lib/extract_couch_docs.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}

var wim_sites,vds_ids

var queue = require('queue-async')

before(function(done){
    var q = queue()
    q.defer(function(cb){
        wim_imputed({'year':2012
                     ,'config_file':config_file}
		    ,function(e,sites){
                        should.not.exist(e)
                        should.exist(sites)
                        sites.should.have.lengthOf(140)
                        wim_sites = sites
                        return cb()
                    })
        return null
    })
    q.defer(function(cb){
        vds_imputed({'year':2012
                     ,'config_file':config_file}
		    ,function(e,sites){
                        should.not.exist(e)
                        sites.should.have.lengthOf(493)
                        vds_ids = sites
                        return cb()
                    })
        return null
    })
    q.await(function(e,r){

        return done()

    })
    return null
})

describe('extract wim and wim_dir',function(){
    it('should extract the WIM sites from couch docs',function(done){
        var sites = extract_couch_docs.extract_wim_sites(wim_sites)
        sites.should.be.instanceOf(Array).and.have.lengthOf(97)
        sites.forEach(function(s){
            s.should.be.a.Number
            return null
        })
        return done()
    })
    it('should extract the WIM sites and dirs from couch docs',function(done){
        var sites = extract_couch_docs.extract_wim_site_dirs(wim_sites)
        sites.should.be.instanceOf(Array).and.have.lengthOf(140)
        sites.forEach(function(s){
            s.should.be.instanceOf(Array).and.have.lengthOf(2)
            s[0].should.be.a.Number
            s[1].should.be.a.String
            return null
        })
        return done()
    })
    it('should extract the VDS sites from couch docs',function(done){
        var ids = extract_couch_docs.extract_vds_ids(vds_ids)
        ids.should.be.instanceOf(Array).and.have.lengthOf(493)
        ids.forEach(function(s){
            s.should.be.a.Number
            return null
        })
        return done()
    })

})
