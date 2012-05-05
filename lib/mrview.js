


/**

    View interface.

    -   add ( doc )
    -   remove ( doc )
    -   query ( options )

 **/

    ////    mapper  function ( doc, emitFunc ) -> ( k, v ), ( k, v ), ( k, v ) ...
    ////    reducer function ( [key], [value], rereduce ) : value

    ////    index   
    ////    cache   optional but you need it to make reduce queries go faster

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
        return index.getLength;
    };

    this.query = function ( options )
    {

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

            group       = String ( options.group ) === 'true',
            reduce      = reducer && String ( options.reduce ) !== 'false', // defaults to true

        //  upseq       = options.update_seq,
        //  stale       = options.stale,

            time        = Date.now (),
            nocache     = false,
            tmp,
            rows,
            out;

        if ( options.group_level )
            throw new Error ( "`group_level=#` queries not supported." );
        if ( options.include_docs )
            throw new Error ( "`include_docs=true` queries not supported." );
        if ( reduce && !group )
            throw new Error ( "`reduce=true&group=false` queries not supported." );


            ////    Determine the keyrange.

        if ( desc )
        {
            tmp         = startkey;
            startkey    = endkey;
            endkey      = tmp;

            tmp         = startid;
            startid     = endid;
            endid       = tmp;
        }

        if ( keys )
        {
            rows = index.selectKeys ( keys );
            nocache = true;
        }

        else if ( startkey || endkey )
            rows = index.selectRange ( startkey, startid, endkey, endid, true, !!inclend );

        else
            rows = index;


            ////    Render, reverse and paginate.

        if ( reduce )
            out = [{ key : null, value : reduceRows ( rows, reducer, nocache ? null : cache ) }];

        else
            out = rows.map ( function ( row )
            {
                return { id : row.id, key : JSON.parse ( row.key ), value : JSON.parse ( row.value ) };
            });


        if ( desc )
            out.reverse ();

        if ( skip > 0 )
            out = out.slice ( skip );
        if ( limit > 0 )
            out = out.slice ( 0, limit );


            ////    Format output alla couchdb.

        return { time : Date.now () - time, rows : out };
    };
};



function reduceRows ( rows, reducer, cache )
{
    var n = rows.getLength (),
        a, data, lo, hi,

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
        
        if ( cache && ( data = cache.best ( k0, d0, k1, d1 ) ) )
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
            data = reducer ( null, a, true );
        else
            data = a [ 0 ];

        if ( cache )
            cache.put ( k0, d0, k1, d1, JSON.stringify ( data ) );

        return data;
    }

    else
        return reducer ( rows.mapKeys ( JSON.parse ), rows.mapValues ( JSON.parse ), false );
}


