


var Index = module.exports = function ( rows, min, max )
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
        var i, row;

        for ( i = min; i < max; i ++ )
        {
            row = rows [ i ];

            if ( row.key > key )
                return -1;
            if ( row.key === key && row.id === id )
                return i - min;
        }

        return -1;
    };

    this.selectKeys = function ( keys )
    {
        if ( !sorted ) sort ();

        keys.sort ();
        var out = [], i,
            min = keys [ 0 ],
            max = keys [ keys.length - 1 ],
            key;

        for ( i = min; i < max; i ++ )
        {
            key = rows [ i ].key;
            if ( key < min )
                continue;
            if ( key > max )
                return out;

            if ( keys.indexOf ( key ) > -1 )
                out.push ( rows [ i ] );
        }

        return new Index ( out );
    };

    this.selectRange = function ( k0, d0, k1, d1, incl0, incl1 )
    {
        if ( !sorted ) sort ();

        var i,
            row, key, id,
            a = min - 1, b = max;

        for ( i = min; i < max; i ++ )
        {
            row = rows [ i ];
            key = row.key;
            id  = row.id;

            if ( k0 && ( key < k0 || ( key === k0 && ( id < d0 || ( ( id === d0 || !d0 ) && !incl0 ) ) ) ) )
            {
                a = i;
                continue;
            }

            if ( k1 && ( key > k1 || ( key === k1 && ( id > d1 || ( ( id === d1 || !d1 ) && !incl1 ) ) ) ) )
            {
                b = i;
                break;
            }
        }

        return new Index ( rows, a + 1, b );
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
    };
};

function Row ( x, y, z )
{
    this.key    = x;
    this.id     = y;
    this.value  = z;
}


