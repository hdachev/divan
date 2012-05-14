

var fakeredis   = require ( 'fakeredis' ),
    vows        = require ( 'vows' ),
    should      = require ( 'should' ),

    client      = fakeredis.createClient (),
    divan       = require ( '../index' ),

    makeDB      = function ( client )
    {
        return divan.makeDivan ({ aof : divan.makeRedisAOF ({ client : client, bucket : 'TEST', name : 'test' }) });
    };


vows.describe ( "redis AOF" )

.addBatch
({
    "persist" :
    {
        topic : function ()
        {
            var db = makeDB ( client ),
                cb = this.callback;

            db.save ({ _id : '1' });
            db.save ({ _id : '2' });
            db.save ({ _id : '3' });

            db.get ( 'ready', function ()
            {
                client.getKeyspace ( { map : true }, cb );
            });
        },

        "writes" : function ( err, data )
        {
            should.not.exist ( err );
            Object.keys ( data ).length.should.equal ( 2 );
            data [ 'TEST.aof' ].length.should.equal ( 1 );
            data [ 'TEST.aof' + '-' + data [ 'TEST.aof' ] [ 0 ] ].should.equal ( '{"_id":"1"}\n{"_id":"2"}\n{"_id":"3"}\n' );
        }
    }
})

.addBatch
({
    "restore" :
    {
        topic : function ()
        {
            var db = makeDB ( client ), docs = [];
            db.forEach
            (
                function ( data )
                {
                    docs.push ( data );
                },
                this.callback.bind ( this, null, docs )
            );
        },

        "reads" : function ( err, data )
        {
            should.not.exist ( err );

            data.sort ( function ( a, b )
            {
                return Number ( a._id ) - Number ( b._id );
            });

            should.deepEqual ( data, [ {_id:"1"}, {_id:"2"}, {_id:"3"} ]);
        }
    }
})

.export ( module );

