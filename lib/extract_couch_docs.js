var _ = require('lodash')

function extract_vds_ids(couch_docs){
    var vds_ids = couch_docs.map(function(v,k){
        return +v.id
    })
    return _.uniq(vds_ids,true)
}

function extract_wim_sites(couch_docs){
    var wim_sites = couch_docs.map(function(w,k){
        var site_dir = w.id.split('.').slice(1)
        return +site_dir[0]
    })
    return _.uniq(wim_sites,true)
}

function extract_paired_wim(couch_docs){
    // what is the year?
    // first key

    var year = couch_docs[0].key[0]

    var paired_sites = couch_docs.filter(function(w,k){
        var i = w.doc[year + ''].imputed
        var m = w.doc[year + ''].merged
        return (i==='finished' &&
                m !== undefined &&
                m !== 'nopair')
    })
    return paired_sites
}

function extract_paired_wim_sites(couch_docs){
    var paired_sites = extract_paired_wim(couch_docs)
    return extract_wim_sites(paired_sites)
}

function extract_notpaired_wim(couch_docs){
    // what is the year?
    // first key

    var year = couch_docs[0].key[0]

    var npaired_sites = couch_docs.filter(function(w,k){
        var i = w.doc[year + ''].imputed
        var m = w.doc[year + ''].merged
        return (i==='finished' &&
                (m === undefined ||
                 m === 'nopair'))
    })
    return npaired_sites
}

function extract_notpaired_wim_sites(couch_docs){
    var npaired_sites = extract_notpaired_wim(couch_docs)
    return extract_wim_sites(npaired_sites)
}

module.exports.extract_vds_ids = extract_vds_ids
module.exports.extract_wim_sites = extract_wim_sites
module.exports.extract_paired_wim_sites = extract_paired_wim_sites
module.exports.extract_notpaired_wim_sites = extract_notpaired_wim_sites

module.exports.extract_paired_wim = extract_paired_wim
module.exports.extract_notpaired_wim = extract_notpaired_wim
