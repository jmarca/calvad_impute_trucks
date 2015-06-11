var wim_sites = require('calvad_wim_sites')

function get_wim_merged(opts,cb){
    opts.couchdb.include_docs=true
    wim_sites.get_wim_merged(opts,function(e,r){
        cb(e,r.rows)
        return null
    })
    return null
}

module.exports = get_wim_merged
