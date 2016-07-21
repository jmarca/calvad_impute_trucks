/*global console */
var vds_sites = require('calvad_vds_sites')

function get_vds_imputed(opts,cb){
    vds_sites.get_vds_done_raw_imputing(opts,function(e,r){
        // no need to filter, already doing that in the library fn
        // I used to filter based on starting id, but that is a bad idea
        // for this one
        cb(e,r.rows)

        return null
    })
    return null
}

function get_vds_truckimputed_todo(opts,cb){
    var minimum_id = 0
    if(opts.calvad !== undefined && opts.calvad.start_vdsid !== undefined){
        minimum_id = +opts.calvad.start_vdsid
    }

    opts.couchdb.include_docs=true
    opts.couchdb.startkey=[+opts.year]
    opts.couchdb.endkey=[+opts.year,'\ufff0']
    vds_sites.get_vds_status_truckimputation(opts,function(e,docs){
        // only return the ones that are not finished
        if(docs.rows === undefined || docs.rows.length === 0){
            return cb(null,docs.rows)
        }
        console.log("Caution:  config.calvad.truckimpute_redo is not truthy, but you've also set a value of "+minimum_id+" for a starting vdsid.  Double check that this is correct.  The imputation step might be skipping some vdsids that have not done their truck impute step.  This is probably okay, but this warning message is here to make sure you double check.")

        var return_rows = docs.rows.filter(function(row,i){
            // decide based on the keys of row

            // view defaults to raw completed, what is truck status


            if(row.key[1] === 'unprocessed' && +row.id >= minimum_id){
                return true
            }
            return false
        })
        console.log('got '+docs.rows.length+' todo docs from couchdb; returning ',return_rows.length +' docs greater than the start vdsid of '+minimum_id)
        return cb(e,return_rows)
    })
    return null
}


module.exports = get_vds_imputed
module.exports.get_vds_truckimputed_todo = get_vds_truckimputed_todo
