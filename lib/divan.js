

    ////    The divan.

module.exports = function ( options )
{
    var self    = this,


        ////    Config & state.

        docs    = options.keyspace  || new Plainspace,
        filter  = options.filter,

        aof     = options.aof,
        snap    = options.snapshot,
        sint    = Math.max ( Number ( options.snapshotInterval ) || 15 * 60 * 1000, 60 * 1000 ),

        cold    = options.views     || {},
        warm    = {},

        logger  = options.logger    || ( options.verbose && console ) || { log : function () {} },

        tostr   = options.stringify || JSON.stringify,
        parse   = options.parse     || JSON.parse,

        waiting = [], ready, flushing, timeout,


        ////    Private.

        flush = function ()
        {
            var rev;

            timeout = 0;
            if ( flushing )
                return;

            logger.log ( "DIVAN> Producing a fresh snapshot ..." );

            flushing = true;
            rev = snap.create ( filter ? new Filter ( docs, filter ) : docs, function ( err )
            {
                flushing = false;

                    ////    A failed snapshot should break the application.

                if ( err )
                    throw err;

                    ////    Append-only files get compacted only on successful snapshot.

                if ( aof )
                    aof.allowCompact ();
            });

            if ( !rev )
                throw new Error ( "Falsy revstring." );

            if ( aof )
                aof.create ( rev );
        },

        get = function ( key )
        {
            var str = docs.get ( key );
            return str && parse ( str );
        },

        save = function ( doc )
        {
            var key, str, cur,
                prop, view;

                ////    Validate.

            if ( !doc ) throw new Error ( "Falsy doc." );
            if ( !( key = doc._id ) ) throw new Error ( "Falsy key." );
            if ( typeof key !== 'string' ) throw new Error ( "Key is not a string." );

                ////    Reject.

            if ( filter && !filter ( doc ) ) return;

                ////    Put in warm views and keyspace.

            if ( !doc._deleted )
                str = tostr ( doc );

            if ( warm )
            {
                cur = docs.get ( key );
                for ( prop in warm )
                    if ( ( view = warm [ prop ] ) && !view.$skip && view.add )
                    {
                            ////    Remove old and add new.
                            ////    Parse every time to protect the documents from getting modified.

                        if ( cur )
                            view.remove ( parse ( cur ) );
                        if ( str )
                            view.add ( parse ( str ) );
                    }
            }

            if ( str )
                docs.set ( key, str );
            else
                docs.del ( key );

                ////    Persist.

            if ( ready )
            {
                    ////    Append-only driver.

                if ( aof )
                    aof.append ( str );

                    ////    Snapshot driver.

                if ( !timeout && snap )
                    timeout = setTimeout ( flush, sint );
            }
        },

        warmView = function ( name )
        {
            var view;

            if ( !( view = warm [ name ] ) )
            {
                if ( cold && ( view = cold [ name ] ) )
                {
                    logger.log ( "\nDIVAN> Warming up " + name + " ..." );

                    warm [ name ] = view;
                    delete cold [ name ];

                    docs.forEach ( function ( docstr )
                    {
                        var doc = parse ( docstr );
                        view.add ( doc );
                    });

                    if ( view.warmUp )
                        view.warmUp ();
                }

                else
                    throw new Error ( "No such view - " + name + "." );
            }

            return view;
        };


        ////    Boot.

    ( function ()
    {
        var storeDoc, setReady, readAOF,
            count = 0, scount = 0;

        logger.log ( "DIVAN> Starting ..." );

        storeDoc = function ( str )
        {
            count ++;
            save ( parse ( str ) );
        };

        setReady = function ()
        {
            var a = waiting, i, n = a.length;

            ready = true;
            waiting = null;

            logger.log ( "DIVAN> AOF yields " + ( count - scount ) + " entries." );

            if ( snap )
            {
                if ( count > scount )
                    flush ();

                else
                    logger.log ( "DIVAN> Snapshot is up to date." );
            }

            if ( n )
                logger.log ( "\nDIVAN> Executing a queue of " + n + " delayed calls to the DB." );

            for ( i = 0; i < n; i ++ )
                a [ i ] ();
        };

        readAOF = function ( err, rev )
        {
            if ( err )
                throw err;
            if ( !rev && rev !== null )
                throw new Error ( "Falsy, non-null snapshot revision." );

            scount = count;
            if ( scount )
            {
                if ( !rev )
                    throw new Error ( "Falsy revision when snapshot has documents." );

                logger.log ( "DIVAN> " + scount + " documents from snapshot @rev " + rev );
            }
            else if ( snap )
                logger.log ( "DIVAN> No documents from snapshot." );

            if ( aof )
            {
                logger.log ( "DIVAN> Reading AOF ..." );
                if ( rev )
                    aof.seek ( rev, true );

                aof.forEach ( storeDoc, setReady );
            }
            else
                setReady ();
        };

            ////    Start.

        if ( snap )
        {
            logger.log ( "DIVAN> Reading snapshot ..." );
            snap.forEach ( storeDoc, readAOF );
        }
        else
            readAOF ( null, null );
    }
    () );


        ////    Public.

    self.get = function ( key, callback )
    {
        if ( ready )
            callback ( null, get ( key ) );
        else
            waiting.push ( self.get.bind ( self, key, callback ) );

        return self;
    };
        
    self.save = function ( doc )
    {
        if ( ready )
            save ( doc );
        else
            waiting.push ( self.save.bind ( self, doc ) );

        return self;
    };

    self.view = function ( name, options, callback )
    {
        if ( !callback && typeof options === 'function' )
        {
            callback = options;
            options = null;
        }

        if ( !options )
            options = {};

        if ( options.include_docs )
            callback = includeDocs ( docs, callback );

        if ( ready )
            callback ( null, warmView ( name ).query ( options ) );
        else
            waiting.push ( self.view.bind ( self, name, options, callback ) );

        return self;
    };

    self.forEach = function ( onData, onDone )
    {
        if ( ready )
            docs.forEach ( function ( doc ) { onData ( parse ( doc ) ); }, onDone );
        else
            waiting.push ( self.forEach.bind ( self, onData, onDone ) );

        return self;
    };

    self.addView = function ( name, follows, view )
    {
        if ( follows && !view )
        {
            view = follows;
            follows = null;
        }

            ////    TODO: make view chains lazy.

        if ( follows )
        {
            view.$skip = true;
            warm [ name ] = view;
            follows.forEach ( function ( name )
            {
                if ( warm [ name ] )
                    warm [ name ].chain ( view );

                else if ( cold [ name ] )
                {
                    cold [ name ].chain ( view );

                    if ( ready )
                        warmView ( name );
                    else
                        waiting.push ( warmView.bind ( null, name ) );
                }
            });
        }

        else
            cold [ name ] = view;

        return self;
    };


        ////    Aliases.

    self.add = function ( doc )
    {
        self.save ( doc );
        return self;
    };

    self.remove = function ( obj )
    {
        self.save ({ _id : obj._id || obj, _deleted : true });
        return self;
    };


        ////    Experimental.

    self.stats = function ()
    {
        var key, stats =
        {
            proc : process.memoryUsage (),
            docs : docs && docs.stats (),
            warm : {}
        };

        if ( warm )
            for ( key in warm )
                stats.warm [ key ] = warm [ key ].stats ();

        return stats;
    };

    self.flush = function ()
    {
        if ( ready ) flush ();
        return self;
    };

    self.filter = function ( func )
    {
        filter = func; // todo: filter the DB immediately.
        return self;
    };

};


    ////    Default keyspace, nothing special.

function Plainspace ()
{
    var data = Object.create ( null );

    this.set = function ( key, value )
    {
        data [ key ] = value;
    };

    this.del = function ( key )
    {
        delete data [ key ];
    };

    this.get = function ( key )
    {
        return data [ key ];
    };

    this.forEach = function ( onData, onDone )
    {
        var key;
        for ( key in data )
            onData ( data [ key ] );
        if ( onDone )
            onDone ();
    };

    this.stats = function ()
    {
        var x = 0, key;
        for ( key in data )
            x ++;

        return x;
    };
}


    ////    Keyspace filter.

function Filter ( docs, filter )
{
    this.forEach = function ( onData, onDone )
    {
        docs.forEach
        (
            function ( data )
            {
                if ( filter ( data ) )
                    onData ( data );
            },
            onDone
        );
    };
}


    ////    Include docs.

function includeDocs ( keyspace, callback )
{
    return function ( err, data )
    {
        var a, i, n;

        if ( data && ( a = data.rows ) && ( n = a.length ) )
            for ( i = 0; i < n; i ++ )
                if ( a [ i ] && a [ i ].id )
                    a [ i ].doc = keyspace.get ( a [ i ].id );

        callback ( err, data );
    };
}





