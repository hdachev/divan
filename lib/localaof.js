

/**

    interface File
    {
        readSince ( funcOnStr, rev, funcOnDrain )
        append ( str )
        mark ( rev )
    }

 **/


    ////    AOF implementation based on multiple local append-only files, one per DB revision.

var File = require ( "./file" );

exports.LocalAOF = function ( options )
{
    var self    = this,

        dir     = options.dir,
        name    = options.name || 'aof',
        compact = options.compact,

        files   = File.list ( dir, name ),
        current = files [ files.length - 1 ];


        ////    Read in all data since the provided revision.

    self.forEach = function ( onData, onDone )
    {
        var list, next;

        list = !current ? files.concat () : files.filter ( function ( file )
        {
            return file.rev >= current.rev;
        });

        next = function ()
        {
            var file = list.shift ();
            if ( file )
                file.forEach ( onData, next );
            else
                onDone ();
        };

        next ();
    };


        ////    Append.

    self.append = function ( line )
    {
        if ( !current )
        {
            current = File.make ( dir, name );
            files.push ( current );
        }
        
        current.append ( line );
    };


        ////    Start a new file.

    self.seek = function ( rev )
    {
        if ( !rev || !( rev = String ( rev ) ) )
            throw new Error ( "Bad rev : " + rev );

        if ( current && current.rev === rev )
            return;

        files.forEach ( function ( file )
        {
            if ( !( rev > file.rev ) )
                throw new Error ( "Rev is not incremental, new : " + rev + ", found : " + file.rev );

            if ( compact && file !== current )
                file.unlink ();
        });

        current = File.make ( dir, name, rev );
        files.push ( current );
    };
};



