


    ////    mapper  function ( doc, callback ) -> ( k, v ), ( k, v ), ( k, v ) ...
    ////    reducer function ( [key], [value], rereduce ) : value
    ////    index   see mindex.js
    ////    cache   optional, see rcache.js

module.exports = function ( mapper, reducer, index, cache )
{

    this.add = function ( doc )
    {
        var id = doc._id,
            keys = {};

        mapper ( doc, function ( k, v )
        {
            var key = JSON.stringify ( k );

            if ( !keys )
                throw new Error ( "Async emit." );
            if ( keys [ key ] )
                throw new Error ( "Duplicate key '" + key + "' for docId '" + id + "'." );

            keys [ key ] = true;
            index.add ( key, id, JSON.stringify ( v ) );

            if ( cache )
                cache.invalidate ( key, id );
        });

        keys = null;
    };

    this.remove = function ( doc )
    {
        var ok = true;

        mapper ( doc, function ( k, v )
        {
            var key = JSON.stringify ( k );

            if ( !ok )
                throw new Error ( "Async emit." );

            index.remove ( key, doc._id );
        });

        ok = false;
    };

    this.stats = function ()
    {
        return { index : index && index.stats (), cache : cache && cache.stats () };
    };

    this.query = function ( options )
    {
        /**profile**/ rcount = 0; rtime = 0; cget = 0, cgett = 0, cput = 0, cputt = 0; /**/

            ////    as per http://wiki.apache.org/couchdb/HTTP_view_API

        var keys        = ( options.key    && [ JSON.stringify ( options.key ) ] )
                       || ( options.keys   && options.keys.map ( function ( k ) { return JSON.stringify ( k ); } ) ),

            startkey    = options.startkey && JSON.stringify ( options.startkey ),
            endkey      = options.endkey   && JSON.stringify ( options.endkey ),
            startid     = options.startkey_docid,
            endid       = options.endkey_docid,

            inclend     = String ( options.inclusive_end ) !== 'false', // defaults to true
            desc        = String ( options.descending ) === 'true',
            limit       = Number ( options.limit ),
            skip        = Number ( options.skip ),

            reduce      = reducer && String ( options.reduce ) !== 'false', // defaults to true
            group       = reduce  && String ( options.group )  === 'true',  // defaults to false

            ranges,
            rows,
            out,

            /**profile**/ count = 0, t0, t1, t2, t3, /**/

            tmp;

        if ( options.group_level )
            throw new Error ( "`group_level=#` queries not supported." );

        if ( desc )
        {
            tmp         = startkey;
            startkey    = endkey;
            endkey      = tmp;

            tmp         = startid;
            startid     = endid;
            endid       = tmp;
        }

        /**profile**/ t0 = Date.now (); /**/


            ////    Build up the keyranges.


        if ( keys )
        {
            ranges = keys.map ( function ( key )
            {
                var rows;
                rows = index.selectRange ( key, null, key, null, true, true );
                rows.$key = key;
                /**profile**/ count += rows.getLength (); /**/
                return rows;
            });
        }

        else
        {
            if ( startkey || endkey )
            {
                rows = index.selectRange ( startkey, startid, endkey, endid, !desc || ( desc && inclend ), desc || ( !desc && inclend ) );
                ranges = [ rows ];
                /**profile**/ count = rows.getLength (); /**/
            }
            else
            {
                ranges = [ index ];
                /**profile**/ count = index.getLength (); /**/
            }

            if ( reduce && group )
                ranges = splitRanges ( ranges );
        }

        /**profile**/ t1 = Date.now (); /**/


            ////    Render.

        out = [];

        ranges.forEach ( function ( rows, i )
        {
            if ( reduce )
                out.push ({ key : ( group && rows.$key && JSON.parse ( rows.$key ) ) || null, value : reduceRows ( rows, reducer, cache ) });

            else
                out.push.apply ( out, rows.map ( function ( row )
                {
                    return { id : row.id, key : JSON.parse ( row.key ), value : JSON.parse ( row.value ) };
                }));
        });

        if ( reduce && !group && out.length > 1 )
            out = [{ key : null, value : reducer ( null, out.map ( function ( row ) { return row.value; } ) ) }];

        /**profile**/ t2 = Date.now (); /**/


            ////    Order and limit.    

        if ( desc )         out.reverse ();
        if ( skip > 0 )     out = out.slice ( skip );
        if ( limit > 0 )    out = out.slice ( 0, limit );


            ////    Output with stats.

        /**profile**/
        t3 = Date.now ();
        return {
            rows    : out,
            matches : count,
            select  : t1 - t0,
            render  : t2 - t1,
            output  : t3 - t2,
            total   : t3 - t0,
            reduce  : { count : rcount, time : rtime },
            rcache  :
            {
                get : { count : cget, time : cgett },
                put : { count : cput, time : cputt }
            }
        };
        /**/

        return { rows : out };
    };

    /**profile**/
    var rtime   = 0,
        rcount  = 0,
        redfunc = reducer;
    reducer     = function ( a, b, c )
    {
        rcount ++;
        var t0  = Date.now (),
            out = redfunc ( a, b, c );
        rtime  += Date.now () - t0;
        return out;
    }
    /**/

};



/**profile**/
var cget = 0, cgett = 0, cput = 0, cputt = 0;
/**/

function reduceRows ( rows, reducer, cache )
{
    var n = rows.getLength (),
        a, data, lo, hi,

        /**profile**/ t0, /**/

        head, tail,
        k0, d0, k1, d1;

        ////    We try to reduce about 20 to 40 values at once.

    if ( n > 39 )
    {
        head = rows.getHead ();
        k0 = head.key;
        d0 = head.id;

        tail = rows.getTail ();
        k1 = tail.key;
        d1 = tail.id;

        if ( cache )
        {
            /**profile**/ cget ++; t0 = Date.now (); /**/
            data = cache.best ( k0, d0, k1, d1 );
            /**profile**/ cgett += Date.now () - t0 /**/
        }

        if ( data )
        {
            a  = [ JSON.parse ( data.value ) ];
            lo = rows.indexOf ( data.a0, data.b0 );
            hi = 1 + rows.indexOf ( data.a1, data.b1 );

            if ( lo < 0 || lo > n - 1 )
                throw new Error ( "lo: " + lo );
            if ( hi < 1 || hi > n )
                throw new Error ( "hi: " + hi );
            if ( hi < lo )
                throw new Error ( "hi<lo" );
        }

        else
        {
            a  = [];
            lo = hi = Math.floor ( n / 2 );
        }

        if ( lo > 0 )
            a.unshift ( reduceRows
            (
                rows.slice ( 0, lo ),
                reducer,
                cache
            ));

        if ( hi < n )
            a.push ( reduceRows
            (
                rows.slice ( hi ),
                reducer,
                cache
            ));

        if ( a.length > 1 )
        {
            data = reducer ( null, a, true );

            if ( cache )
            {
                /**profile**/ cput ++; t0 = Date.now (); /**/
                cache.put ( k0, d0, k1, d1, JSON.stringify ( data ) );
                /**profile**/ cputt += Date.now () - t0 /**/
            }
        }

        else
            data = a [ 0 ];

        return data;
    }

    else
        return reducer ( rows.mapKeys ( JSON.parse ), rows.mapValues ( JSON.parse ), false );
}



function splitRanges ( ranges )
{
    var out = [];

    ranges.forEach ( function ( range )
    {
        var slice, key;

        while ( range.getLength () )
        {
            key   = range.getHead ().key;
            slice = range.selectRange ( key, null, key, null, true, true );
            slice.$key = key;
            range = range.slice ( slice.getLength () );
            out.push ( slice );
        }
    });

    return out;
}


