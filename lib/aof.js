

    ////    AOF implementation based on multiple local append-only files, one per DB revision.

module.exports = function ( options, File )
{
    var self        = this,
        waiting     = [],

        dir         = options.dir,
        name        = options.name || 'aof',
        compact     = options.compact;

    File.list ( dir, name, function ( err, files )
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
            });

            current = File.make ( dir, name, rev );
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


////////

        var a = waiting, n = a.length, i;
        waiting = null;
        for ( i = 0; i < n; i ++ )
            a [ i ] ();
    });

    if ( !waiting )
        return;

    self.forEach = function ()
    {
        var a = Array.prototype.slice.apply ( arguments );
        waiting.push ( function ()
        {
            self.forEach.apply ( self, a );
        });
    };

    self.append = function ()
    {
        var a = Array.prototype.slice.apply ( arguments );
        waiting.push ( function ()
        {
            self.append.apply ( self, a );
        });
    };

    self.seek = function ()
    {
        var a = Array.prototype.slice.apply ( arguments );
        waiting.push ( function ()
        {
            self.seek.apply ( self, a );
        });
    };
};






