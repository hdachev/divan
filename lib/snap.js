

/**

    interface Snapshot
    {
        forEach ( funcOnLine, funcOnDrain )
        revstr create ( keyspace )
    }

 **/


    ////    Snapshot implementation based on multiple local append-only files, one per DB revision.

module.exports = function ( options, File )
{
    var self        = this,
        waiting     = [],

        dir         = options.dir,
        name        = options.name || 'snap',
        compact     = options.compact,
        compress    = options.compress;

    files = File.list ( dir, name, function ( err, files )
    {

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

        self.create = function ( keyspace, errback )
        {
            var rev = String ( Date.now () ),
                file;

            files.forEach ( function ( file )
            {
                if ( !( file.rev < rev ) )
                    throw new Error ( "A previous snapshot has a higher rev string." );
            });

            file = File.make ( dir, name, rev );
            file.atomic ();

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
                        errback ( err );

                    else
                        file.commit ( function ( err )
                        {
                            errback ( err );

                                ////    Compact commit history only after the commit is ready.

                            if ( !err && compact && files.length > 2 )
                                files.splice ( 0, files.length - 2 ).forEach ( function ( file )
                                {
                                    file.unlink ();
                                });
                        });
                }
            );

                ////    Return the new revstr asap so that an AOF can move on immediately.

            return rev;
        };


////////

        var a = waiting, n = a.length, i;
        waiting = null;
        for ( i = 0; i < n; i ++ )
            a [ i ] ();
    });

    if ( !waiting )
        return;

    self.forEach = function ( a, b )
    {
        waiting.push ( function ()
        {
            self.forEach ( a, b );
        });
    };

    self.create = function ( a )
    {
        waiting.push ( function ()
        {
            self.create ( a );
        });
    };
};



