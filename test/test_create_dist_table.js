/* global require console process describe it */

var should = require('should')
var create_dist_table = require('../lib/create_dist_table.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)


var config_file = rootdir+'/../test.config.json'
var config={}
var config_okay = require('config_okay')
var wim_imputed = require('../lib/wim_imputed.js')
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
        q.defer(vds_imputed,opts)
        q.await(function(e,w,v){
            config.wim_sites = w
            config.vds_sites = v
            return done()

        })
        return null
    })
    return null
})

describe('query wim vds distances',function(){
    it('should get distance table for wim vds',function(done){
        create_dist_table(config,function(e,r){
            var numrecords = Object.keys(r).length
            numrecords.should.equal(117)
            var check_queue = queue(5)
            var toolong = 0
            Object.keys(r).forEach(function(wim_site){
                // there is at least one site with distance 25,000+
                // that was used as a pair in 2009
                if(wim_site,r[wim_site][0].dist > 26000){
                    toolong++
                    check_queue.defer(function(cb){
                        var opts = {
                            'doc':wim_site
                            ,'year':2009
                            ,'state':'paired'
                            ,'record':r[wim_site][0]
                        }
                        _.assign(opts,config.couchdb)
                        couch_check(
                            opts
                            ,function(err,state){
                                should.not.exist(err)
                                should.exist(state)
                                state.should.equal('none')
                                return cb()

                            })
                        return null
                    })
                }
                return null
            })
            check_queue.await(function(e,r){
                toolong.should.equal(31)
                return done(e)
            })

        })
    })

})
