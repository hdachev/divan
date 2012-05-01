

    ////    The divan coordinator.

exports.Divan = function ( options )
{
    console.log ( "DIVAN> Starting ..." );

    var self    = this,

        docs    = options.keyspace || new Plainspace,
        filter  = options.filter,
        aof     = options.aof,
        views   = options.views,
        snap    = options.snapshot,

        tostr   = options.serialize || function ( obj )
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
                console.log ( e );
            }
        },

        ready,
        timeout,
        get, save, view,
        flush;

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

        if ( filter && !filter ( doc ) )
            return;

            ////    Views.

        if ( views )
        {
            cur = self.get ( key );
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
                timeout = setTimeout ( flush, Math.max ( Number ( self.snapshotInterval ) || 0, 60 * 1000 ) );
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
        timeout = 0;
        rev = snap.create ( docs );
        if ( rev && aof )
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

            console.log ( "DIVAN> AOF yields " + ( count - scount ) + " entries." );

            if ( snap )
            {
                if ( count > scount )
                {
                    console.log ( "DIVAN> Producing a fresh snapshot ..." );
                    flush ();
                }
                else
                    console.log ( "DIVAN> Snapshot is up to date." );
            }

            if ( n )
                console.log ( "\nDIVAN> Executing a queue of " + n + " delayed calls to the DB." );

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

                console.log ( "DIVAN> " + scount + " documents from snapshot @rev " + rev );
            }
            else if ( snap )
                console.log ( "DIVAN> No documents from snapshot." );

            if ( aof )
            {
                console.log ( "DIVAN> Reading AOF ..." );
                if ( rev )
                    aof.seek ( rev );

                aof.forEach ( storeDoc, setReady );
            }
            else
                setReady ();
        };

        if ( snap )
        {
            console.log ( "DIVAN> Reading snapshot ..." );
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
    }
    () );
};


    ////    Interfaces.

/**

    AOF interface

    -   forEachSince ( funcOnStr, rev, funcOnDrain )
    -   append ( str )
    -   seek ( rev )

 **/

/**

    Snapshot interface

    -   forEach ( funcOnStr, funcOnDrain )
    -   rev put ( arrayOfStr )

 **/

/**

    View interface.

    -   add ( doc )
    -   remove ( doc )
    -   query ( options )

 **/

/**

    Keyspace interface

    -   get ( key )
    -   put ( key, value )
    -   forEach ( online, ondone )

 **/

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
}



