var extract_couch_docs = require('../lib/extract_couch_docs.js')
var pg = require('pg')

function setup_connection(opts){

    var host = opts.postgresql.host ? opts.postgresql.host : '127.0.0.1';
    var user = opts.postgresql.auth.username ? opts.postgresql.auth.username : 'myname';
    //var pass = opts.postgresql.auth.password ? opts.postgresql.password : '';
    var port = opts.postgresql.port ? opts.postgresql.port :  5432;
    var db  = opts.postgresql.grid_merge_sqlquery_db ? opts.postgresql.grid_merge_sqlquery_db : 'spatialvds'
    var connectionString = "pg://"+user+"@"+host+":"+port+"/"+db
    return connectionString
}

// yet another distance matrix
//
// this time, wim with pairs to wim without

function create_distance_table(opts,cb){
    // use postgresql to get a distance table from all known WIM ids
    // to all known VDS ids that are good for this year

    // create teh big sql statement
    var p_site_docs  = extract_couch_docs.extract_paired_wim(opts.wim_sites)
    var psites = extract_couch_docs.extract_wim_sites(p_site_docs)

    var np_site_docs = extract_couch_docs.extract_notpaired_wim(opts.wim_sites)
    var npsites = extract_couch_docs.extract_wim_sites(np_site_docs)

    // this is needed to filter out spurious combinations of wim and direction
    var wim_paired_site_ids = p_site_docs.map(function(doc){
        return doc.id.slice(4)
    })
    var wim_notpaired_site_ids = np_site_docs.map(function(doc){
        return doc.id.slice(4)
    })

    var wim_sites_a =
            "a as (select * from wim_geoview w where site_no < 800 and "+
            "site_no || '.' || direction in ('" +
            wim_paired_site_ids.join("','") +
            "'))"


    var wim_sites_b =
            "b as (select * from wim_geoview w where site_no < 800 and "+
            "site_no || '.' || direction in ('" +
            wim_notpaired_site_ids.join("','") +
            "'))"

    var wim_wim_dist =
            'wim_wim_dist as ('+
            ' select distinct '+
            ' a.site_no as paired_site_no,'+
            ' b.site_no as notpaired_site_no,'+
            ' newctmlmap.canonical_direction(a.direction) as paired_direction,'+
            ' newctmlmap.canonical_direction(b.direction) as notpaired_direction,'+
            ' ST_Distance_Sphere(a.geom,b.geom) as dist'+
            ' from a'+
            ' left outer join b on  (1=1) )'

    var with_statement =
            'with '+
            wim_sites_a+', '+
            wim_sites_b+', '+
            wim_wim_dist

    var select_statement =
            'SELECT * ' + //'INTO TEMP vds_wim_distance'+
            ' FROM wim_wim_dist'+
            ' WHERE paired_site_no is not null'+
            ' AND notpaired_site_no is not null'+
            ' ORDER BY notpaired_site_no,dist,paired_site_no,notpaired_direction;'


    var query = with_statement + ' ' + select_statement

    var connectionString = setup_connection(opts)

    pg.connect(connectionString
               ,function(err,client,clientdone){
                   if(err) return cb(err)
                   client.query(query,function(e,r){
                       if(e){

                           console.log('query failed',query)
                           console.log(e)
                           clientdone()
                           return cb(e)

                       }
                       // have the distance table as a list of records

                       // convert to a hashtable

                       var hashmap = {

                       }
                       r.rows.forEach(function(row){
                           var paired_cdbwimid = 'wim.'+row.paired_site_no+'.'+
                                   row.paired_direction.substr(0,1)
                                     .toUpperCase()
                           var notpaired_cdbwimid = 'wim.'+
                                   row.notpaired_site_no+'.'+
                                   row.notpaired_direction.substr(0,1)
                                     .toUpperCase()
                           if(hashmap[notpaired_cdbwimid] === undefined){
                               hashmap[notpaired_cdbwimid] = [row]
                           }else{
                               hashmap[notpaired_cdbwimid].push(row)
                           }
                           return null
                       })
                       cb(null,hashmap)
                       clientdone()
                       return null
                   })
                   return null
               })
    return null
}

module.exports=create_distance_table
