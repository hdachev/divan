


/**

    ReduceCache interface.

    -   put  ( a0, b0, a1, b1, value )
    -   best ( a0, b0, a1, b1 ) : { a0, b0, a1, b1, value }
    -   null invalidate ( a, b )

 **/

    ////    A cache is optional but you need it to make reduce queries go faster.
    ////    This one is simplistic but good enough for testing.
    ////    Perhaps swap it for something more elaborate later, like a B* tree or something.

module.exports = function ()
{
    var index = [];

    this.put = function ( a0, b0, a1, b1, value )
    {
        index.push ( new Entry ( a0, b0, a1, b1, value ) );
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

    this.best = function ( a0, b0, a1, b1 )
    {
        var i, n = index.length,
            entry, best, x;

        for ( i = 0; i < n; i ++ )
        {
            entry = index [ i ];

            if ( entry.a0 < a0 || ( entry.a0 === a0 && entry.b0 < b0 ) )
                continue;
            if ( entry.a1 > a1 || ( entry.a1 === a1 && entry.b1 > b1 ) )
                continue;

            if ( !best || ( ( best.a0 >= entry.a0 || ( best.a0 === entry.a0 && best.b0 >= entry.b0 ) ) && ( best.a1 <= entry.a1 || ( best.a1 === entry.a1 && best.b1 <= entry.b1 ) ) ) )
            {
                best = entry;
                x = i;
            }
        }

        if ( best )
        {
                ////    We throw the value away to help the cache rearrange in case the keyspace is changing.

            index.splice ( x, 1 );
            return { a0 : best.a0, b0 : best.b0, a1 : best.a1, b1 : best.b1, value : best.value };
        }

        return null;
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


