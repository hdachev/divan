

/**

    interface Snapshot
    {
        forEach ( funcOnStr, funcOnDrain )
        rev create ( arrayOfStr )
    }

 **/


    ////    Snapshot implementation based on multiple local append-only files, one per DB revision.

var File = require ( "./file" );

exports.LocalSnapshot = function ( options )
{
    var self    = this,

        dir     = options.dir,
        name    = options.name || 'snap',
        compact = options.compact,

        onFile  = options.onFile,

        files   = File.list ( dir, name );


        ////    Read in all data from the last snapshot.

    self.forEach = function ( onData, onDone )
    {
        var file = files [ files.length - 1 ];

        if ( !file )
            onDone ( null, null );

        else if ( !file.rev )
            throw new Error ( "Bad file revision : " + file );

        else
            file.forEach ( onData, function ()
            {
                onDone ( null, file.rev );
            });
    };


        ////    Create a new snapshot from a keyspace object.

    self.create = function ( keyspace )
    {
        var rev = String ( Date.now () ),
            file;

        files.forEach ( function ( file )
        {
            if ( !( file.rev < rev ) )
                throw new Error ( "A previous snapshot has a higher rev string." );
        });

        file = File.make ( dir, name, rev );
        files.push ( file );

        keyspace.forEach
        (
            function ( data )
            {
                file.append ( data );
            },
            function ( err )
            {
                if ( err )
                    throw err;

                file.close ();
                if ( onFile )
                    onFile ( file.getPath () );

                if ( compact && files.length > 2 )
                    files.splice ( 0, files.length - 2 ).forEach ( function ( file )
                    {
                        file.unlink ();
                    });
            }
        );

            ////    Return the new revision.

        return rev;
    };
};



