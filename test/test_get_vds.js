/* global require console process describe it */

var should = require('should')

var get_vds = require('../lib/get_vds')
var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = path.normalize(rootdir+'/../test.config.json')
var config={}


describe('get vds',function(){
    it('should get all the vds sites done imputing for 2010',function(done){
        get_vds({'year':2010
                ,'config_file':config_file
                ,'funky':'music'}
               ,function(e,r){
                    should.not.exist(e)
                    should.exist(r)
                    r.should.have.lengthOf(5392)
                    r.forEach(function(task){
                        task.should.have.property('funky','music')
                        task.should.have.property('year',2010)
                        task.should.have.property('vdsid')
                        task.vdsid.should.match(/^\d+$/)
                    })
                    return done()
                })
    })
    it('should return an empty list for 2014',function(done){
        get_vds({'year':2014
                ,'config_file':config_file}
               ,function(e,r){
                    should.not.exist(e)
                    should.exist(r)
                    r.should.have.lengthOf(0)
                    return done()
                })
    })
})
