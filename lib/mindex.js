

var Index,
    LEXMAX = String.fromCharCode ( 0x1fffffffffffff );

    ////

module.exports = Index = function ( rows, min, max )
{
    var sorted = true,
        sort;

    if ( !rows ) rows = [];
    if ( !min )  min = 0;
    if ( !max )  max = rows.length;


        ////    Add / remove.

    this.add = function ( key, id, value )
    {
        sorted = false;
        rows.push ( new Row ( key, id, value ) );
        max ++;
    };

    this.remove = function ( key, id )
    {
        var x;
        while (( x = this.indexOf ( key, id ) ))
        {
            rows.splice ( x, 1 );
            max --;
        }
    };


        ////    Searching and selection.

    this.indexOf = function ( key, id )
    {
        if ( !sorted ) sort ();
        var pos = search ( rows, min, max, { key : key, id : id }, order ),
            row = rows [ pos ];

        if ( row.key === key && id === id )
            return pos - min;
        else
            return -1;
    };

    this.selectRange = function ( k0, d0, k1, d1, inclK0, inclK1 )
    {
        if ( !sorted ) sort ();

        var a = min,
            b = max;

        ////    As per http://wiki.apache.org/couchdb/HTTP_view_API
        ////    inclusive_end - Controls whether the endkey is included in the result. It defaults to true.

        if ( k0 ) a = search ( rows, a, b, { key : k0 + ( !inclK0 ? LEXMAX : '' ), id : d0 || '' }, order );
        if ( k1 ) b = search ( rows, a, b, { key : k1 + (  inclK1 ? LEXMAX : '' ), id : ( d1 || '' ) + LEXMAX }, order );

        return new Index ( rows, a, b );
    };


        ////    Alla-buffer slicing.

    this.get = function ( x )
    {
        if ( !sorted ) sort ();
        return rows [ min + x ];
    };

    this.stats =
    this.getLength = function ()
    {
        return max - min;
    };

    this.mapKeys = function ( func )
    {
        if ( !sorted ) sort ();
        var out = [], i;

        for ( i = min; i < max; i ++ )
            out.push ( func ( rows [ i ].key ) );

        return out;
    };

    this.mapValues = function ( func )
    {
        if ( !sorted ) sort ();
        var out = [], i;

        for ( i = min; i < max; i ++ )
            out.push ( func ( rows [ i ].value ) );

        return out;
    };

    this.getHead = function ()
    {
        if ( !sorted ) sort ();
        return rows [ min ];
    };

    this.getTail = function ()
    {
        if ( !sorted ) sort ();
        return rows [ max - 1 ];
    };

    this.slice = function ( a, b )
    {
        if ( !sorted ) sort ();
        if ( !a ) a = 0;
        if ( !b ) b = max - min;

        return new Index ( rows, a + min, b + min );
    };

    this.map = function ( func )
    {
        var out = [], i;
        for ( i = min; i < max; i ++ )
            out.push ( func ( rows [ i ] ) );

        return out;
    };


        ////    Lazy sort.

    sort = function ()
    {
        if ( min > 0 || max < rows.length )
            throw new Error ( "Sorting a slice." );

        sorted = true;
        rows.sort ( order );
    };
};



function Row ( x, y, z )
{
    this.key    = x;
    this.id     = y;
    this.value  = z;
}



function order ( a, b )
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
}

function search ( array, from, to, value, compare )
{
    var pivot = Math.floor ( ( from + to ) / 2 ),
        order = compare ( value, array [ pivot ] );

    if ( order > 0 )
    {
        if ( pivot === from )
            return to;
        else
            return search ( array, pivot, to, value, compare );
    }

    else
    {
        if ( pivot === from )
            return from;
        else
            return search ( array, from, pivot, value, compare );
    }
}


