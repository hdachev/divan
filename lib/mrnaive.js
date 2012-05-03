


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
            mapper ( doc, function ( id, key, value )
            {
                if ( doc._id !== id )
                    throw new Error ( "Bad document id emitted." );

                var x = JSON.stringify ( key ),
                    y = JSON.stringify ( id ),
                    z = JSON.stringify ( value );

                sorted = false;
                index.push ( new Row ( x, y, z ) );
            });
        },

        remove : function ( doc )
        {
            mapper ( doc, function ( id, key, value )
            {
                if ( doc._id !== id )
                    throw new Error ( "Bad document id emitted." );

                var x = JSON.stringify ( key ),
                    y = JSON.stringify ( id ),
                    pos;

                while ( ( pos = findRow ( x, y ) ) > -1 )
                    index.splice ( pos, 1 );
            });
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

                group       = options.group_level || ( String ( options.group ) === true && 0xffffff ),
                reduce      = reducer && String ( options.reduce ) !== 'false', // defaults to true
                include     = String ( options.include_docs ) === true,

            //  upseq       = options.update_seq,
            //  stale       = options.stale,

                tmp,
                data;

            if ( group )
                throw new Error ( "NAIVEMR> `group` queries not implemented." );
            if ( include )
                throw new Error ( "NAIVEMR> `include_docs` queries not implemented." );


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


                ////    Reverse and paginate.

            if ( desc )
                data.reverse ();

            if ( skip > 0 )
                data = data.slice ( skip );
            if ( limit > 0 )
                data = data.slice ( 0, limit );


                ////    Format output alla couchdb.

            return data;
        }

    };
};



function Row ( x, y, z )
{
    this.key    = x;
    this.id     = y;
    this.value  = z;
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
        if ( a.y < b.y )
            return -1;
        if ( a.y > b.y )
            return 1;

        return 0;
    });
}



function selectKeys ( rows, keys )
{
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
    var out = [], i, n = rows.length,
        row, key, id;

    for ( i = 0; i < n; i ++ )
    {
        row = rows [ i ];
        key = row.key;
        id  = row.id;

        if ( key < k0 || ( key === k0 && ( id < d0 || ( ( id === d0 || !d0 ) && !incl0 ) ) ) )
            continue;
        if ( key > k1 || ( key === k1 && ( id > d1 || ( ( id === d1 || !d1 ) && !incl1 ) ) ) )
            return out;

        out.push ( rows [ i ] );
    }

    return out;
}



function listKeys ( rows )
{
    var out = [], i, n = rows.length;
    for ( i = 0; i < n; i ++ )
        out.push ( rows [ i ].key );

    return out;
}

function listValues ( rows )
{
    var out = [], i, n = rows.length;
    for ( i = 0; i < n; i ++ )
        out.push ( rows [ i ].value );

    return out;
}

function reduceRows ( keys, values, reducer )
{
    var out = [],
        i, n = values.length;

    for ( i = 0; i < n; i += 100 )
        out.push
        (
            reducer // func ( keys, values )
            (
                ( keys && keys.splice ( i, i + 100 ) ) || null,
                values.slice ( i, i + 100 )
            )
        );

        ////    We always re-reduce, even if its just one value.

    if ( out.length > 1 || keys )
        return reduceRows ( null, out, reducer );

    else
        return out;
}




