


/**

    View interface.

    -   add ( doc )
    -   remove ( doc )
    -   query ( options )

 **/

    ////    Naive implementation of the map-reduce fun in couchdb.
    ////    Should work the way you'd expect it to, but its very slow.

module.exports = function ( mapper, reducer )
{
    var index   = [],
        sorted  = false;

    return {

        add : function ( doc )
        {
            var sync = true;
            mapper ( doc, function ( key, value )
            {
                if ( !sync )
                    throw new Error ( "Async emit." );

                sorted = false;
                index.push ( new Row ( key, doc._id, value ) );
            });
            sync = false;
        },

        remove : function ( doc )
        {
            var sync = true;
            mapper ( doc, function ( key, value )
            {
                if ( !sync )
                    throw new Error ( "Async emit." );

                var pos;
                while ( ( pos = findRow ( key, doc._id ) ) > -1 )
                    index.splice ( pos, 1 );
            });
            sync = false;
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
                tmp,
                data;

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
                data = selectKeys ( index, keys );
            else if ( startkey || endkey )
                data = selectRange ( index, startkey, endkey, startid, endid, true, !!inclend );
            else
                data = index;


                ////    Reduce if necessary.
                ////    Grouping can fit in here nicely.

            if ( reduce )
                data = [{ key : null, value : reduceRows ( listKeys ( data ), listValues ( data ), reducer ) }];

            else
                data = data.map ( function ( row )
                {
                    return { id : row.id, key : JSON.parse ( row.key ), value : JSON.parse ( row.value ) };
                });


                ////    Reverse and paginate.

            if ( desc )
                data.reverse ();

            if ( skip > 0 )
                data = data.slice ( skip );
            if ( limit > 0 )
                data = data.slice ( 0, limit );


                ////    Format output alla couchdb.

            return { time : Date.now () - time, rows : data };
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

    key = JSON.stringify ( key );

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
        if ( a.y < b.y )
            return -1;
        if ( a.y > b.y )
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
        {
            console.log ( "Bailing at " + key + " / " + id + " @" + out.length );
            return out;
        }

        out.push ( row );
    }

    console.log ( out.lenth + " rows selected." );

    return out;
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

function reduceRows ( keys, values, reducer )
{
    var pivot = Math.floor ( values.length / 2 );

    if ( pivot > 50 )
        return reducer
        (
            null,
            [
                reduceRows
                (
                    keys.slice ( 0, pivot ),
                    values.slice ( 0, pivot ),
                    reducer
                ),
                reduceRows
                (
                    keys.slice ( pivot ),
                    values.slice ( pivot ),
                    reducer
                )
            ],
            true
        );

    else
        return reducer
        (
            keys,
            values,
            false
        );
}


