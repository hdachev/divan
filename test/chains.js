

module.exports = function ( docCount, module )
{

    var vows    = require ( 'vows' ),
        should  = require ( 'should' ),
        _       = require ( 'underscore' ),

        divan   = require ( '../index' ),
        db      = divan.makeDivan ({});


        ////

    db.addView ( 'warm/reduce', divan.mr
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

    db.addView ( 'warm/chain-map', [ 'warm/reduce' ], divan.mr
    (
        function ( doc, emit )
        {
            emit ( doc.value, doc._id );
        }
    ));


        ////

    db.addView ( 'cold/reduce', divan.mr
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


        ////

    function makedoc ( num )
    {
        db.save ({ _id : 'doc-' + num, sqrt : num * num, odd : num % 2 });
    }

    var ALL_SQ_SUM  = _.reduce ( _.range ( docCount ), function ( memo, a ) { return memo + a*a; }, 0 ),
        ODD_SQ_SUM  = _.reduce ( _.range ( 1, docCount, 2 ), function ( memo, a ) { return memo + a*a; }, 0 );


        ////

    vows.describe ( "chains @" + docCount + " docs" )

    .addBatch
    ({
        "empty db" :
        {
            topic : function ()
            {
                db.view ( 'warm/reduce', this.callback );
            },

            "reduces to an empty rowset" : function ( err, data )
            {
                should.not.exist ( err );
                should.deepEqual ( data.rows, [] );
            }
        }
    })


        ////

    .addBatch
    ({
        "warm populate" :
        {
            topic : function ()
            {
                _.range ( docCount ).forEach ( makedoc );

                db.view ( 'warm/reduce', { group : true }, this.callback );
            },

            "reduces right" : function ( err, data )
            {
                should.not.exist ( err );
                if ( docCount > 1 )
                    should.deepEqual
                    (
                        data.rows,
                        [
                            { key : 'all', value : ALL_SQ_SUM },
                            { key : 'odd', value : ODD_SQ_SUM }
                        ]
                    );

                else
                    should.deepEqual
                    (
                        data.rows,
                        [
                            { key : 'all', value : ALL_SQ_SUM }
                        ]
                    );
            }
        }
    })

    .addBatch
    ({
        "warm chained-map" :
        {
            topic : function ()
            {
                db.view ( 'warm/chain-map', this.callback );
            },

            "works right" : function ( err, data )
            {
                should.not.exist ( err );

                if ( docCount > 2 )
                    should.deepEqual
                    (
                        data.rows,
                        [
                            { key : ODD_SQ_SUM, id : 'odd', value : 'odd' },
                            { key : ALL_SQ_SUM, id : 'all', value : 'all' }
                        ]
                    );

                else if ( docCount > 1 )
                    should.deepEqual
                    (
                        data.rows,
                        [
                            { key : ALL_SQ_SUM, id : 'all', value : 'all' },
                            { key : ODD_SQ_SUM, id : 'odd', value : 'odd' }
                        ]
                    );

                else
                    should.deepEqual
                    (
                        data.rows,
                        [
                            { key : ALL_SQ_SUM, id : 'all', value : 'all' }
                        ]
                    );
            }
        }
    })

    .addBatch
    ({
        "warm doc updates" :
        {
            topic : function ()
            {
                db.forEach ( function ( doc )
                {
                    if ( doc.odd )
                        db.remove ( doc );
                });

                db.view ( 'warm/chain-map', this.callback );
            },

            "update chains correctly" : function ( err, data )
            {
                should.not.exist ( err );
                should.deepEqual
                (
                    data.rows,
                    [{ key : ALL_SQ_SUM - ODD_SQ_SUM, id : 'all', value : 'all' }]
                );
            }
        }
    })

    .addBatch
    ({
        "cold reduce" :
        {
            topic : function ()
            {
                db.view ( 'cold/reduce', { group : true }, this.callback );
            },

            "works right" : function ( err, data )
            {
                should.not.exist ( err );
                should.deepEqual
                (
                    data.rows,
                    [{ key : 'all', value : ALL_SQ_SUM - ODD_SQ_SUM }]
                );
            }
        }
    })

    .addBatch
    ({
        "cold chained-map" :
        {
            topic : function ()
            {
                db.addView ( 'cold/chain-map', [ 'cold/reduce' ], divan.mr
                (
                    function ( doc, emit )
                    {
                        emit ( doc.value, doc._id );
                    }
                ));

                db.view ( 'cold/chain-map', this.callback );
            },

            "works right" : function ( err, data )
            {
                should.not.exist ( err );
                should.deepEqual
                (
                    data.rows,
                    [{ key : ALL_SQ_SUM - ODD_SQ_SUM, id : 'all', value : 'all' }]
                );
            }
        }
    })


        ////

    .export ( module );
};


