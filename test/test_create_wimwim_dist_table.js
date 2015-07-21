/* global require console process describe it */

var should = require('should')
var create_ww_dist_table = require('../lib/create_wim_wim_dist_table.js')
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
        q.defer(wim_imputed,opts)
        q.defer(wim_merged,opts)
        q.await(function(e,w_i,w_m){
            console.log('done with prep')
            config.wim_imputed_sites = w_i
            config.wim_merged_sites = w_m
            return done()

        })
        return null
    })
    return null
})

describe('query wim wim distances',function(){
    it('should get distance table for wim vds',function(done){
        config.wim_sites = config.wim_imputed_sites
        console.log('wim_imputed_sites length',config.wim_imputed_sites.length)
        console.log('wim_merged_sites length',config.wim_merged_sites.length)
        create_ww_dist_table(config,function(e,r){
            var numrecords = Object.keys(r).length
            numrecords.should.equal(46)    // 55 imputed but
                                           // non-merged sites minus 9
                                           // 800 series sites === 46
            Object.keys(r).forEach(function(site_no){
                var len = r[site_no].length
                len.should.equal(85)       // 85 merged sites
                return null
            })
            return done()
        })
    })

})
