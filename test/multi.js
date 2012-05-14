

module.exports = function ( docCount, module )
{

    var vows    = require ( 'vows' ),
        should  = require ( 'should' ),
        _       = require ( 'underscore' ),

        divan   = require ( '../index' ),
        db      = divan.makeDivan ({});


    _.range ( 0, docCount ).forEach ( function ( num )
    {
        db.save ({ _id : 'doc-' + num, sqrt : num * num, odd : num % 2 });
    });


    db.addView ( 'A', divan.mr
    (
        function ( doc, emit )
        {
            emit ( 'all', doc.sqrt );
            if ( doc.odd )
                emit ( 'odd', doc.sqrt );
        },
        function ( k, v )
        {
            if ( !v.length )
                throw new Error ( "EMPTY VALUES SET" );

            return v.reduce ( function ( a, b )
            {
                return ( a || 0 ) + ( b || 0 );
            });
        }
    ));

    db.addView ( 'B', divan.mr
    (
        function ( doc, emit )
        {
            emit ( 'all', doc.sqrt );
            if ( doc.odd )
                emit ( 'odd', doc.sqrt );
        },
        function ( k, v )
        {
            if ( !v.length )
                throw new Error ( "EMPTY VALUES SET" );

            return v.reduce ( function ( a, b )
            {
                return ( a || 0 ) + ( b || 0 );
            });
        }
    ));


    db.addView ( 'C', [ 'A' ], divan.mr
    (
        function ( doc, emit )
        {
            emit ( 'L-' + doc._id.length, 1 );
        },
        function ( k, v )
        {
            return v.reduce ( function ( a, b ) { return a + b } );
        }
    ));

    db.addView ( 'D', [ 'B', 'C' ], divan.mr
    (
        function ( doc, emit )
        {
            emit ( 1000000 + doc.value, doc._id );
        }
    ));


    var ALL_SQ_SUM  = _.reduce ( _.range ( docCount ), function ( memo, a ) { return memo + a*a; }, 0 ),
        ODD_SQ_SUM  = _.reduce ( _.range ( 1, docCount, 2 ), function ( memo, a ) { return memo + a*a; }, 0 );


    vows.describe ( "multi @" + docCount + " docs" ) .addBatch
    ({
        "chains with multiple sources" :
        {
            topic : function ()
            {
                db.view ( 'D', this.callback );
            },

            "collect right" : function ( err, data )
            {
                should.not.exist ( err );
                should.deepEqual
                (
                    data.rows,
                    [
                        { key : 1000000 + 2,          value : 'L-3', id : 'L-3' },
                        { key : 1000000 + ODD_SQ_SUM, value : 'odd', id : 'odd' },
                        { key : 1000000 + ALL_SQ_SUM, value : 'all', id : 'all' }
                    ]
                );
            }
        }
    })
    .export ( module );
};


