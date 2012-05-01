

var fs = require ( "fs" ),
    File;


    ////    Most of the append-only file logic from node-dirty.

File = function ( dir, file )
{
    var path = dir + '/' + file,
        writer;

    this.append = function ( line )
    {
        if ( !writer )
            writer = fs.createWriteStream ( path, { encoding : 'utf-8', flags : 'a' } );

        writer.write ( line + '\n' );
    };

    this.end = function ()
    {
        writer = { write : function () { throw new Error ( "This file is closed." ); } };
    };

    this.unlink = function ()
    {
        this.end ();
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

exports.list = function ( dir, name, callback )
{
    if ( !name || !/^[a-zA-Z0-9]+$/.test ( name ) )
        throw new Error ( "Bad filename : " + name );

    fs.readdir ( dir, function ( err, filenames )
    {
        if ( err )
            throw err;

        var files   = [],
            pattern = new RegExp ( '^' + name + '.([a-z0-9]+)$' );

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


exports.make = function ( dir, name, rev )
{
    if ( !name || !/^[a-zA-Z0-9]+$/.test ( name ) )
        throw new Error ( "Bad filename : " + name );

    if ( !rev )
        rev = '0';

    var file = new File ( dir, name + '.' + rev );
        file.rev = String ( rev );

    return file;
};



