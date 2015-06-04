/*global console */
var vds_sites = require('calvad_vds_sites')

function get_vds_imputed(opts,cb){
    vds_sites.get_vds_done_raw_imputing(opts,function(e,r){
        // no need to filter, already doing that in the library fn
        cb(e,r.rows)

        return null
    })
    return null
}

module.exports = get_vds_imputed
