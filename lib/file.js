

var fs = require ( "fs" ),
    File,

    UNCOMMITED = '.uncommitted';


    ////    Most of the append-only file logic from node-dirty.

File = function ( dir, file )
{
    var path = dir + '/' + file,
        writer;


    this.atomic = function ()
    {
        if ( writer )
            throw new Error ( "Already writing." );

        path += UNCOMMITED;
    };

    this.append = function ( line )
    {
        if ( !writer )
            writer = fs.createWriteStream ( path, { encoding : 'utf-8', flags : 'a' } );

        writer.write ( line + '\n' );
    };

    this.commit = function ( errback )
    {
        var path0 = path,
            path1 = path.replace ( UNCOMMITED, '' );

        if ( path0 === path1 )
            throw new Error ( "Not atomic." );

        if ( writer && writer.destroySoon )
        {
            writer.on ( 'close', function ()
            {
                fs.rename ( path0, path1, errback );
            });

            writer.destroySoon ();
        }
        else
            throw new Error ( "Already committed." );

        path = path1;
        writer = { write : function () { throw new Error ( "This file is closed." ); } };
    };


    this.unlink = function ()
    {
        writer = { write : function () { throw new Error ( "This file is unlinked." ); } };
        fs.unlink ( path, function () {} );
    };


    this.forEach = function ( onData, onDone )
    {
        var buffer = '';

        fs.createReadStream ( path, { encoding: 'utf-8', flags : 'r' })
            .on ( 'error', function ( err )
            {
                if ( err.code === 'ENOENT' )
                    onDone ();
                else
                    throw err;
            })
            .on ( 'data', function ( chunk )
            {
                var arr;

                buffer += chunk;
                if ( chunk.lastIndexOf ( '\n' ) < 0 )
                    return;

                arr = buffer.split ( '\n' );
                buffer = arr.pop ();
                arr.forEach ( function ( rowStr )
                {
                    rowStr.trim ();
                    if ( rowStr )
                        onData ( rowStr );
                });
            })
            .on ( 'end', function ()
            {
                if ( buffer.length )
                    throw new Error ( "Corrupted row at the end of the db : " + buffer );

                onDone ();
            });
    };


    this.toString = function ()
    {
        return '[File ' + path + ']';
    };
};


    ////    File operations.

exports.list = function ( dir, name, ext, callback )
{
    if ( !name || !/^[a-zA-Z0-9_.-]+$/.test ( name ) )
        throw new Error ( "Bad filename : " + name );
    if ( !ext || !/^[a-zA-Z0-9]+$/.test ( ext ) )
        throw new Error ( "Bad extension : " + name );

    fs.readdir ( dir, function ( err, filenames )
    {
        if ( err )
            throw err;

        var files   = [],
            pattern = new RegExp ( '^' + name + '.([a-z0-9]+).' + ext + '$' );

        filenames.forEach ( function ( filename )
        {
            var matches, file;
            if ( !( matches = pattern.exec ( filename ) ) )
                return;

            file = new File ( dir, filename );
            file.rev = matches [ 1 ];
            files.push ( file );
        });

        files.sort ( function ( a, b )
        {
            return a.rev < b.rev ? -1 : a.rev > b.rev ? 1 : 0;
        });

        callback ( null, files );
    });
};

exports.make = function ( dir, name, ext, rev )
{
    if ( !name || !/^[a-zA-Z0-9_.-]+$/.test ( name ) )
        throw new Error ( "Bad filename : " + name );
    if ( !ext || !/^[a-zA-Z0-9]+$/.test ( ext ) )
        throw new Error ( "Bad extension : " + name );

    if ( !rev )
        rev = '0';

    var file = new File ( dir, name + '.' + rev + '.' + ext );
        file.rev = String ( rev );

    return file;
};



