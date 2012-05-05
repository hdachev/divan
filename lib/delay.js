

var slice = Array.prototype.slice;

module.exports = function ( obj )
{
    var waiting = [],
        methods = slice.call ( arguments, 1 ),
        i, n = methods.length;

    for ( i = 0; i < n; i ++ )
        obj [ methods [ i ] ] = makePlaceholder ( obj, methods [ i ], waiting );

    return function ()
    {
        var a = waiting,
            i, n = a.length;

        if ( !a ) throw new Error ( "Already called." );
        a.push = function ()
        {
            throw new Error ( "Placeholder should have been overwritten by now." );
        };

        waiting = null;
        for ( i = 0; i < n; i ++ )
            a [ i ] ();
    };
};

function makePlaceholder ( obj, method, waiting )
{
    return function ()
    {
        var args = slice.call ( arguments );
        waiting.push ( function ()
        {
            obj [ method ].apply ( obj, args );
        });
    };
};

