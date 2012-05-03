

    ////    The divan.

exports.Divan = function ( options )
{
    var self    = this,

        docs    = options.keyspace || new Plainspace,
        filter  = options.filter,
        aof     = options.aof,
        views   = options.views,
        snap    = options.snapshot,
        sint    = Number ( options.snapshotInterval ) || 15 * 60 * 1000,

        logger  = ( options.verbose && console ) || { log : function () {} },

        tostr   = options.stringify || function ( obj )
        {
            return JSON.stringify ( obj );
        },

        parse   = options.parse || function ( str )
        {
            if ( str ) try
            {
                return JSON.parse ( str );
            }
            catch ( e )
            {
                logger.log ( e, 'in doc', str );
            }
        },

        ready, flushing,

        timeout,
        get, save, view,
        flush;

    logger.log ( "DIVAN> Starting ..." );

    get = function ( key )
    {
        var str = docs.get ( key );
        return str && parse ( str );
    };

    save = function ( doc )
    {
        var key, cur,
            prop, view,
            str;

            ////    Validate.

        if ( !doc )
            throw new Error ( "Falsy doc." );
        if ( typeof ( key = doc._id ) === 'undefined' )
            throw new Error ( "Falsy key." );
        if ( typeof key !== 'string' )
            throw new Error ( "Key must be a string, key: " + key + ", typeof key: " + typeof key );

        if ( filter && !filter ( doc ) )
            return;

            ////    Views.

        if ( views )
        {
            cur = get ( key );
            for ( prop in views )
                if ( ( view = views [ prop ] ) && view.add )
                {
                        ////    Remove old and add new.

                    if ( cur )
                        view.remove ( cur );

                    view.add ( doc );
                }
        }

            ////    Index.

        str = tostr ( doc );
        docs.set ( key, str );

            ////    Persistance.

        if ( ready )
        {
                ////    Append-only driver.

            if ( aof )
                aof.append ( str );

                ////    Snapshot driver.

            if ( !timeout && snap )
                timeout = setTimeout ( flush, Math.max ( sint, 60 * 1000 ) );
        }
    };

    view = function ( name, options )
    {
        var view = views [ name ];
        if ( !view || !view.query )
            throw new Error ( "No such view." );

        return view.query ( options );
    };

    flush = function ( name, options )
    {
        var rev;

        if ( flushing ) return;
        flushing = true;

        logger.log ( "DIVAN> Producing a fresh snapshot ..." );

        timeout = 0;
        rev = snap.create ( docs, function ( err )
        {
                ////    A failed snapshot should break the application.

            if ( err )
                throw err;
        });

        if ( !rev )
            throw new Error ( "Falsy revstring." );

        if ( aof )
            aof.seek ( rev );
    };

    

    ( function ()
    {
        var waiting = [],
            storeDoc, setReady, readAOF,
            count = 0,
            scount = 0;

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

        if ( snap )
        {
            logger.log ( "DIVAN> Reading snapshot ..." );
            snap.forEach ( storeDoc, readAOF );
        }

        else
            readAOF ( null, null );

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
                callback ( null, view ( name, options ) );
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
                waiting.push ( self.forEach.bind ( self, name, options, callback ) );
        };
    }
    () );

    self.stats = function ()
    {
        var key, stats =
        {
            proc    : process.memoryUsage (),
            docs    : docs && docs.stats (),
            views   : {}
        };

        if ( views )
            for ( key in views )
                stats.views [ key ] = views [ key ].stats ();

        return stats;
    };
};



/**

    Keyspace interface

    -   get ( key )
    -   put ( key, value )
    -   forEach ( online, ondone )

 **/

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




