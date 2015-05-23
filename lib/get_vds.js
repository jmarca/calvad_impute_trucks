/*global console */
var v_s = require('calvad_vds_sites')
var get_vds_done_raw_imputing = v_s.get_vds_done_raw_imputing
var path = require('path');
var rootdir = path.normalize(__dirname)
var config_file = path.normalize(rootdir+'/../config.json')
var _ = require('lodash')

function get_vds(task,cb){
    get_vds_done_raw_imputing({'year':task.year
                              ,'config_file':task.config_file || config_file}
                             ,function(e,r){
                                  if(e) return cb(e)
                                  var result = r.rows.map(function(row,i){
                                                   var v = row.id
                                                   var _opts = _.clone(task)
                                                   _opts.vdsid=v
                                                   return _opts
                                               })
                                  return cb(null,result)
                              })
    return null
}

module.exports=get_vds
