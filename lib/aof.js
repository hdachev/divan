

    ////    AOF implementation based on multiple local append-only files, one per DB revision.

var delay = require ( "./delay" );

module.exports = function ( options, File )
{
    var self        = this,

        dir         = options.dir,
        name        = options.name || 'data',
        compact     = options.compact,

        onReady     = delay ( this, 'forEach', 'append', 'seek' );

    File.list ( dir, name, 'aof', function ( err, files )
    {
        var current = files [ files.length - 1 ];


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
                current = File.make ( dir, name, 'aof' );
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
            });

            current = File.make ( dir, name, 'aof', rev );
            files.push ( current );
        };


            ////    Compact.

        self.allowCompact = function ()
        {
            if ( compact && files.length > 2 )
                files.splice ( 0, files.length - 2 ).forEach ( function ( file )
                {
                    file.unlink ();
                });
        };


            ////    Ready!

        onReady ();
    });
};


