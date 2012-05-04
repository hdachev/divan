


/**

    View interface.

    -   add ( doc )
    -   remove ( doc )
    -   query ( options )

 **/

    ////    mapper    function ( doc, emitFunc ) -> ( k, v ), ( k, v ), ( k, v ) ...
    ////    reducer   function ( [key], [value], rereduce ) : value
    ////    cacher    optional but you need it to make reduce queries go faster

module.exports = function ( mapper, reducer, cache )
{
    var index   = [],
        sorted  = false;

    return {

        add : function ( doc )
        {
            var id = doc._id,
                keys = {};

            mapper ( doc, function ( key, value )
            {
                var row = new Row ( key, id, value );

                if ( !keys )
                    throw new Error ( "Async emit." );
                if ( keys [ row.key ] )
                    throw new Error ( "Duplicate key emitted (" + row.key + ") for document " + id );

                sorted = false;
                keys [ row.key ] = true;
                index.push ( row );

                if ( cache )
                    cache.invalidate ( row.key, id )
            });

            keys = null;
        },

        remove : function ( doc )
        {
            var id = doc._id,
                ok = true;

            mapper ( doc, function ( key, value )
            {
                var pos, row;

                if ( !ok )
                    throw new Error ( "Async emit." );

                key = JSON.stringify ( key );
                while ( ( pos = findRow ( index, key, id ) ) > -1 )
                {
                    row = index [ pos ];
                    index.splice ( pos, 1 );
                }
            });

            ok = false;
        },

        stats : function ()
        {
            return index.length;
        },

        query : function ( options )
        {

                ////    As per http://wiki.apache.org/couchdb/HTTP_view_API

            var keys        = options.keys || ( options.key && [ options.key ] ),

                startkey    = options.startkey,
                endkey      = options.endkey,
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
                rows;

            if ( options.group_level )
                throw new Error ( "`group_level=#` queries not supported." );
            if ( options.include_docs )
                throw new Error ( "`include_docs=true` queries not supported." );
            if ( reduce && !group )
                throw new Error ( "`reduce=true&group=false` queries not supported." );


                ////    Sort if necessary.

            if ( !sorted )
            {
                sorted = true;
                sortRows ( index );
            }


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
                rows = selectKeys ( index, keys );
                nocache = true;
            }

            else if ( startkey || endkey )
                rows = selectRange ( index, startkey, endkey, startid, endid, true, !!inclend );

            else
                rows = index;


                ////    Reduce if necessary.
                ////    Grouping can fit in here nicely.

            if ( reduce )
                rows = [{ key : null, value : reduceRows ( rows, reducer, nocache ? null : cache ) }];

            else
                rows = rows.map ( function ( row )
                {
                    return { id : row.id, key : JSON.parse ( row.key ), value : JSON.parse ( row.value ) };
                });


                ////    Reverse and paginate.

            if ( desc )
                rows.reverse ();

            if ( skip > 0 )
                rows = rows.slice ( skip );
            if ( limit > 0 )
                rows = rows.slice ( 0, limit );


                ////    Format output alla couchdb.

            return { time : Date.now () - time, rows : rows };
        }

    };
};



function Row ( x, y, z )
{
    this.key    = JSON.stringify ( x );
    this.id     = y;
    this.value  = JSON.stringify ( z );
}



function findRow ( rows, key, id )
{
    var i, n = rows.length, row;

    for ( i = 0; i < n; i ++ )
    {
        row = rows [ i ];

        if ( row.key > key )
            return -1;
        if ( row.key === key && row.id === id )
            return i;
    }

    return -1;
}

function sortRows ( rows )
{
    rows.sort ( function ( a, b )
    {
        if ( a.key < b.key )
            return -1;
        if ( a.key > b.key )
            return 1;
        if ( a.id < b.id )
            return -1;
        if ( a.id > b.id )
            return 1;

        return 0;
    });
}



function selectKeys ( rows, keys )
{
    keys = keys.map ( function ( key )
    {
        return JSON.stringify ( key );
    });

    keys.sort ();

    var out = [], i, n = rows.length,
        min = keys [ 0 ],
        max = keys [ keys.length - 1 ],
        key;

    for ( i = 0; i < n; i ++ )
    {
        key = rows [ i ].key;
        if ( key < min )
            continue;
        if ( key > max )
            return out;

        if ( keys.indexOf ( key ) > -1 )
            out.push ( rows [ i ] );
    }

    return out;
}

function selectRange ( rows, k0, k1, d0, d1, incl0, incl1 )
{
    var out = [],
        i, n = rows.length,
        row, key, id;

    if ( k0 ) k0 = JSON.stringify ( k0 );
    if ( k1 ) k1 = JSON.stringify ( k1 );

    console.log ( "Selecting range", k0, k1, d0, d1, incl0, incl1 );

    for ( i = 0; i < n; i ++ )
    {
        row = rows [ i ];
        key = row.key;
        id  = row.id;

        if ( k0 && ( key < k0 || ( key === k0 && ( id < d0 || ( ( id === d0 || !d0 ) && !incl0 ) ) ) ) )
            continue;

        if ( k1 && ( key > k1 || ( key === k1 && ( id > d1 || ( ( id === d1 || !d1 ) && !incl1 ) ) ) ) )
            return out;

        out.push ( row );
    }

    console.log ( out.lenth + " rows selected." );

    return out;
}



function reduceRows ( rows, reducer, cache )
{
    var n = rows.length,
        a, data, lo, hi,

        k0, d0, k1, d1;

        ////    We try to reduce about 20 to 40 values at once.

    if ( n > 39 )
    {
        k0 = rows [ 0 ].key;
        k1 = rows [ n - 1 ].key;
        d0 = rows [ 0 ].id;
        d1 = rows [ n - 1 ].id;
        
        if ( cache && ( data = cache.best ( k0, d0, k1, d1 ) ) )
        {
            a  = [ data.value ];
            lo = findRow ( rows, data.a0, data.b0 );
            hi = 1 + findRow ( rows, data.a1, data.b1 );

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
            cache.put ( k0, d0, k1, d1, data );

        return data;
    }

    else
        return reducer ( listKeys ( rows ), listValues ( rows ), false );
}

function listKeys ( rows )
{
    var out = [], i, n = rows.length;
    for ( i = 0; i < n; i ++ )
        out.push ( JSON.parse ( rows [ i ].key ) );

    return out;
}

function listValues ( rows )
{
    var out = [], i, n = rows.length;
    for ( i = 0; i < n; i ++ )
        out.push ( JSON.parse ( rows [ i ].value ) );

    return out;
}


