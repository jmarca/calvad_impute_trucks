/*global console */
var unique_vds={}
function get_vds(queue,opts,cb){
    return function(e,r){
        console.log(r)
        _.each(r.rows,function(row){
            var v = row.key[1]
            if(unique_vds[v+'.'+opts.year] === undefined){
                var _opts = _.clone(opts)
                _opts.vds=v
                _opts.year=opts.year

                queue.push(_opts
                          ,function(){
                               console.log('vds site '+v+' '+opts.year+' done, '
                                          +queue.length()
                                          +' files remaining')
                                    return null
                                })

                unique_vds[v+'.'+opts.year]=1
            }
            return null
        });
    }
}
