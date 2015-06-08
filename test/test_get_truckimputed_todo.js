/* global require console process describe it */

var should = require('should')

var get_vds = require('../lib/vds_imputed.js')
var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = path.normalize(rootdir+'/../test.config.json')
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

describe('get vds truckimputed',function(){
    it('should get the truck imputed todo for 2012',function(done){
        var opts = config
        opts.year = 2012
        get_vds(opts,function(e,r){
            should.not.exist(e)
            should.exist(r)
            var ready_vds_count = r.length
            get_vds.get_vds_truckimputed_todo(opts,function(e,r){
                should.not.exist(e)
                should.exist(r)
                console.log(r.length,'<?',ready_vds_count)
                var truck_todo = r.length
                truck_todo.should.be.below(ready_vds_count)
                // actually this will change as I process
            // r.should.have.lengthOf(494)
            r.forEach(function(row){
                row.should.have.property('key')
                row.key[1].should.eql('unprocessed')
                return null
            })
            return done()
        })
    })
})
})
