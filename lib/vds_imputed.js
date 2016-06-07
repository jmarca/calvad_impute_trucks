/*global console */
var vds_sites = require('calvad_vds_sites')

function get_vds_imputed(opts,cb){
    vds_sites.get_vds_done_raw_imputing(opts,function(e,r){
        // no need to filter, already doing that in the library fn
        var minimum_id = +opts.calvad.start_vdsid
        // hack.  filter out rows, because when I'm redoing ugh, don't
        // want to have to redo all of them over and over again.
        var return_rows = r.rows.filter(function(row){
            return +row.id >= minimum_id
        })
        cb(e,return_rows)

        return null
    })
    return null
}

function get_vds_truckimputed_todo(opts,cb){
    opts.couchdb.include_docs=true
    opts.couchdb.startkey=[+opts.year]
    opts.couchdb.endkey=[+opts.year,'\ufff0']
    vds_sites.get_vds_status_truckimputation(opts,function(e,docs){
        // only return the ones that are not finished
        if(docs.rows === undefined || docs.rows.length === 0){
            return cb(null,docs.rows)
        }
        var return_rows = docs.rows.filter(function(row,i){
            // decide based on the keys of row

            // view defaults to raw completed, what is truck status

            if(row.key[1] !== 'finished' ){
                return true
            }

            return false
        })
        console.log('rows length returning is ',return_rows.length)
        return cb(e,return_rows)
    })
    return null
}


module.exports = get_vds_imputed
module.exports.get_vds_truckimputed_todo = get_vds_truckimputed_todo
