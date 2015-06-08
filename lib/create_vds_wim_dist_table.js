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

// this is very similar to the one in merge_wim_pairs, except if you
// do not pass in the option "freeway" and "direction" then you get
// all to all.  If you pass in truthy "freeway", then the distance
// table is limited such that wim and vds must be on the same freeway.
// If you pass in truthy direction, then the distance table is limited
// such that the wim and vds on same freeway mustbe going in same
// direction too. If freeway is false or null, the direction option
// does nothing.
//
// another difference is that it sort the results in order of vds
// first, not wim

function create_distance_table(opts,cb){
    // use postgresql to get a distance table from all known WIM ids
    // to all known VDS ids that are good for this year

    // create teh big sql statement
    var sites = extract_couch_docs.extract_wim_sites(opts.wim_sites)
    var vdsids = extract_couch_docs.extract_vds_ids(opts.vds_sites)
    var wim_site_ids = opts.wim_sites.map(function(doc){
        return doc.id
    })
    var latest = 'latest as (select id,max(version) as version from vds_versioned group by id order by id)'
    var vds_geoview =
            'vds_geoview as ('+
            'SELECT v.id,' +
            'v.name,' +
            'v.cal_pm,' +
            'v.abs_pm,' +
            'v.latitude,' +
            'v.longitude,' +
            'vv.lanes,' +
            'vv.segment_length,' +
            'vv.version,' +
            'vf.freeway_id,' +
            'vf.freeway_dir,' +
            'vt.type_id AS vdstype,' +
            'vd.district_id AS district,' +
            'g.gid,' +
            'g.geom' +
            ' FROM vds_id_all v' +
            ' JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id' +
            ' JOIN latest USING(id)' +
            ' JOIN vds_versioned vv ON (vv.id=v.id and vv.version=latest.version)'+
            ' JOIN vds_vdstype vt USING (vds_id)' +
            ' JOIN vds_district vd USING (vds_id)' +
            ' JOIN vds_freeway vf USING (vds_id)' +
            ' JOIN geom_points_4326 g USING (gid)'+
            ' WHERE v.id in ('+
            vdsids.join(',')+
            '))'

    var wim_sites =
            'wim_sites as (select * from wim_geoview w where site_no < 800 and '+
            'site_no in (' +
            sites.join(',') +
            '))'

    var vds_wim_dist =
            'vds_wim_dist as ('+
            ' select distinct v.id as vds_id,w.site_no,'+
            ' v.freeway_id as freeway,'+
            ' newctmlmap.canonical_direction(w.direction) as direction,'+
            ' ST_Distance_Sphere(v.geom,w.geom) as dist'+
            ' from vds_geoview v'+
            ' left outer join wim_sites w on  (1=1) )'

    if(opts.freeway){
        if(opts.direction){
            console.log('querying matching up freeways and direction')
            vds_wim_dist =
                'vds_wim_dist as ('+
                ' select distinct v.id as vds_id,w.site_no,'+
                ' v.freeway_id as freeway,'+
                ' newctmlmap.canonical_direction(w.direction) as direction,'+
                ' ST_Distance_Sphere(v.geom,w.geom) as dist'+
                ' from vds_geoview v'+
                ' left outer join wim_sites w on  ('+
                '            w.freeway_id=v.freeway_id and'+
                '            newctmlmap.canonical_direction(w.direction)='+
                '              newctmlmap.canonical_direction(v.freeway_dir)'+
                '             ))'



        }else{
            console.log('querying matching up freeways without regard to direction')
            vds_wim_dist =
                'vds_wim_dist as ('+
                ' select distinct v.id as vds_id,w.site_no,'+
                ' v.freeway_id as freeway,'+
                ' newctmlmap.canonical_direction(w.direction) as direction,'+
                ' ST_Distance_Sphere(v.geom,w.geom) as dist'+
                ' from vds_geoview v'+
                ' left outer join wim_sites w on  ('+
                '            w.freeway_id=v.freeway_id'+
                '             ))'
        }
    }else{
        console.log('querying without regard to freeways or direction')
    }

    var with_statement =
            'with '+
            latest+', '+
            vds_geoview+', '+
            wim_sites+', '+
            vds_wim_dist

    var select_statement =
            'SELECT * ' + //'INTO TEMP vds_wim_distance'+
            ' FROM vds_wim_dist'+
            ' WHERE site_no is not null'+
            ' ORDER BY vds_id,dist,site_no,direction;'


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
                           var vdsid = row.vds_id
                           var cdbwimid = 'wim.'+row.site_no+'.'+row.direction.substr(0,1).toUpperCase()
                           // The query can generate site-direction
                           // pairs that do *not* actually have valid
                           // data for a year.  For example, if site A
                           // north has good data but south does not,
                           // the distance compute will generate
                           // distances for both north and south for
                           // that site.  The ground truth is the
                           // passed in couchdb wim site ids, that
                           // includes direction as well as the siteno
                           //
                           if(wim_site_ids.indexOf(cdbwimid) === -1){
                               return null
                           }
                           // moving on, stash the row for work
                           if(hashmap[vdsid] === undefined){
                               hashmap[vdsid] = [row]
                           }else{
                               hashmap[vdsid].push(row)
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
