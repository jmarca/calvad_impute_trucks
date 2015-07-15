/* global require console process describe it */

var should = require('should')
var create_dist_table = require('../lib/create_vds_wim_dist_table.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}
var config_okay = require('config_okay')
var wim_imputed = require('../lib/wim_imputed.js')
var wim_merged = require('../lib/wim_merged.js')
var vds_imputed = require('../lib/vds_imputed.js')
var extract_couch_docs = require('../lib/extract_couch_docs.js')
var queue = require('queue-async')
var _ = require('lodash')

var couch_check = require('couch_check_state')

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
        q.defer(wim_merged,opts)
        q.await(function(e,v,w_i,w_m){
            console.log('done with prep')
            config.wim_imputed_sites = w_i
            config.wim_merged_sites = w_m
            config.vds_sites = v
            return done()

        })
        return null
    })
    return null
})

describe('query wim vds distances',function(){
    it('should get distance table for wim vds',function(done){
        config.wim_sites = config.wim_imputed_sites
        create_dist_table(config,function(e,r){
            var numrecords = Object.keys(r).length
            numrecords.should.equal(5873)
            Object.keys(r).forEach(function(vdsid){
                var len = r[vdsid].length
                len.should.equal(140 - 9)  // in 2012, 140 wim sites
                                           // imputed fine, but 9 are
                                           // above 800 series
                                           // (pre-pass type sites)
                return null
            })
            return done()
        })
    })
    it('should get distance table for wim merged, vds',function(done){
        config.wim_sites = config.wim_merged_sites
        create_dist_table(config,function(e,r){
            var numrecords = Object.keys(r).length
            numrecords.should.equal(5873)
            Object.keys(r).forEach(function(vdsid){
                var len = r[vdsid].length
                len.should.equal(85)
                return null
            })
            // var firstvds = Object.keys(r)[0]
            // console.log(r[firstvds].slice(0,10))
            return done()
        })
    })

})
