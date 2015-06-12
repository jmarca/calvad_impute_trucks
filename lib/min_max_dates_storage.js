// parse the output (CSV) dump from truck imputation, and save to
// couchdb status database

get_done_truckimputed(year,cb)

forEach(vdsid){
    if(minmax_not_there(vdsid)){
        thefile = make_filename(vdsid,year)
        process_queue = queue(1) // sequential
        var
        process_queue.defer(loadfile,thefile)

        minmax_dates(data,cb)

save_to_couchdb(min,max,year,vdsid)
