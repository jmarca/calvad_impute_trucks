var wim_sites = require('calvad_wim_sites')

function get_wim_imputed(opts,cb){
    wim_sites.get_wim_imputed_status(opts,function(e,r){
        // get rows, keep ones with "finished"

        var finished = r.rows.filter(function(v,i,a){
            return v.key[1] === 'finished'
        })
        cb(e,finished)
        return null
    })
    return null
}

module.exports = get_wim_imputed
