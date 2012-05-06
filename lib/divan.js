

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
            rev = snap.create ( docs, function ( err )
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
                aof.seek ( rev );
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

            str = tostr ( doc );

            if ( warm )
            {
                cur = docs.get ( key );
                for ( prop in warm )
                    if ( ( view = warm [ prop ] ) && !view.$skip && view.add )
                    {
                            ////    Remove old and add new.
                            ////    Parse every time to protect the documents from getting modified.

                        if ( cur ) view.remove ( parse ( cur ) );
                        view.add ( parse ( str ) );
                    }
            }

            docs.set ( key, str );

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

                    docs.forEach ( function ( doc )
                    {
                        view.add ( parse ( doc ) );
                    });

                    if ( view.warmUp )
                        view.warmUp ();
                }

                else
                    throw new Error ( "No such view." );
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
                    aof.seek ( rev );

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


        ////    Public API.

    self.get = function ( key, callback )
    {
        if ( ready )
            callback ( null, get ( key ) );
        else
            waiting.push ( self.get.bind ( self, key, callback ) );
    };
        
    self.save = function ( doc )
    {
        if ( ready )
            save ( doc );
        else
            waiting.push ( self.save.bind ( self, doc ) );
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

        if ( ready )
            callback ( null, warmView ( name ).query ( options ) );
        else
            waiting.push ( self.view.bind ( self, name, options, callback ) );
    };

    self.flush = function ()
    {
        if ( ready )
            flush ();
    };

    self.forEach = function ( onData )
    {
        if ( ready )
            docs.forEach ( onData );
        else
            waiting.push ( self.forEach.bind ( self, onData ) );
    };

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




