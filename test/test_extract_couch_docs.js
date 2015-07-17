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
var _ = require('lodash')
var config_okay = require('config_okay')

before(function(done){
    config_okay(config_file,function(err,c){
        if(err){
            throw new Error('node.js needs a good croak module')
        }
        config = c
        var q = queue()
        var opts =_.extend({'year':2012},c)
        q.defer(vds_imputed,opts)
        q.defer(wim_imputed,opts)
        q.await(function(e,v,w){
            wim_sites = w
            vds_ids = v
            return done()

        })
        return null
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
    it('should extract the VDS sites from couch docs',function(done){
        var ids = extract_couch_docs.extract_vds_ids(vds_ids)
        ids.should.be.instanceOf(Array).and.have.lengthOf(5873)
        ids.forEach(function(s){
            s.should.be.a.Number
            return null
        })
        return done()
    })

})
