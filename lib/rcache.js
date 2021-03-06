

    ////    A cache is optional but you need it to make reduce queries go faster.
    ////    This one is simplistic but appears to be quite fast.

module.exports = function ()
{
    var index = [];

    this.put = function ( a0, b0, a1, b1, value )
    {
        var i, n = index.length,
            // time = Date.now (),
            entry;

        for ( i = 0; i < n; i ++ )
        {
            entry = index [ i ];
            if ( ( entry.a0 > a0 || ( entry.a0 === a0 && entry.b0 >= b0 ) ) && ( entry.a1 < a1 || ( entry.a1 === a1 && entry.b1 <= b1 ) ) )
            {
                index.splice ( i, 0, new Entry ( a0, b0, a1, b1, value ) );
                // console.log ( "Putting entry at " + i + " / " + n + ", time " + ( Date.now () - time ) );
                return;
            }
        }

        index.push ( new Entry ( a0, b0, a1, b1, value ) );
    };

    this.best = function ( a0, b0, a1, b1 )
    {
        var i, n = index.length,
            // time = Date.now (),
            entry;

        for ( i = 0; i < n; i ++ )
        {
            entry = index [ i ];

            if ( entry.a0 < a0 || ( entry.a0 === a0 && entry.b0 < b0 ) )
                continue;
            if ( entry.a1 > a1 || ( entry.a1 === a1 && entry.b1 > b1 ) )
                continue;

                ////    Remove entries that don't match the requested intervals
                ////        to help the cache re-align to a changing keyspace.

            if ( entry.a0 > a0 || entry.b0 > b0 || entry.a1 < a1 || entry.b1 < b1 )
                index.splice ( i, 1 );

            // console.log ( "Found entry at " + i + " / " + n + ", time " + ( Date.now () - time ) );
            return entry;
        }

        return null;
    };

    this.invalidate = function ( a, b )
    {
        var i, n = index.length,
            entry;

        for ( i = 0; i < n; i ++ )
        {
            entry = index [ i ];

            if ( entry.a0 > a || ( entry.a0 === a && entry.b0 > b ) )
                continue;
            if ( entry.a1 < a || ( entry.a1 === a && entry.b1 < b ) )
                continue;

            index.splice ( i, 1 );
            i --;
            n --;
        }
    };

    this.stats = function ()
    {
        return index.length;
    };
};



function Entry ( a0, b0, a1, b1, value )
{
    this.a0 = a0;
    this.b0 = b0;
    this.a1 = a1;
    this.b1 = b1;

    this.value = value;
}


