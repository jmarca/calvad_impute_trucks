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

function get_vds_truckimputed_todo(opts,cb){
    opts.couchdb.include_docs=true
    opts.couchdb.startkey=[+opts.year,'unprocessed']
    opts.couchdb.endkey=[+opts.year,'unprocessed\ufff0']
    vds_sites.get_vds_status_truckimputation(opts,function(e,docs){
        // only return the ones that are not finished
        if(docs.rows === undefined || docs.rows.length === 0){
            return cb(null,docs.rows)
        }
        var return_rows = docs.rows.filter(function(row,i){
            // decide based on the keys of row
            if(row.doc[opts.year].vdsraw_max_iterations === undefined &&
               row.doc[opts.year].vdsraw_chain_lengths === undefined){
                    return false
            }
            if(row.doc[opts.year].vdsraw_max_iterations === 0 &&
               Array.isArray(row.doc[opts.year].vdsraw_chain_lengths) &&
               row.doc[opts.year].vdsraw_chain_lengths.length>0){
                // ready, but not todo if already done!
                if(row.key[1] === 'unprocessed'){
                    return true
                }
            }
            return false
        })
        return cb(e,return_rows)
    })
    return null
}


module.exports = get_vds_imputed
module.exports.get_vds_truckimputed_todo = get_vds_truckimputed_todo
