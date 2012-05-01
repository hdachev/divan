

/** NEEDS FIXING:
 *  The read comes in as a big chunk - writes should be ok though.
 */

var zlib = require ( "zlib" ),
    fs   = require ( "fs" ),
    util = require ( "util" );


module.exports = function ( S3 )
{

    var File = function ( bucket, file )
    {
        var writer,
            output,
            tmpfile;

        this.append = function ( line )
        {
            if ( !writer )
            {
                tmpfile = './tmp' + ( ( Math.random () * 0xffffff ) & 0xffffff ).toString ( 16 );
                writer = fs.createWriteStream ( tmpfile, { encoding : null, flags : 'w' } );
            }

            writer.write ( line + '\n' );
        };

        this.end = function ()
        {
            var out = writer;
            writer = { write : function () { throw new Error ( "This file is closed." ); } };

            if ( !out || !out.destroySoon )
                return;

            out.destroySoon ();
            out.on ( 'close', function ()
            {
                fs.stat ( tmpfile, function ( err, stats )
                {
                    if ( stats && stats.size )
                        putObject
                        (
                            S3, bucket, file, stats.size,
                            fs.createReadStream ( tmpfile, { encoding : null, flags : 'r' } ),
                            function ( err, data )
                            {
                                if ( err )
                                    throw err;

                                fs.unlink ( tmpfile );
                            }
                        );

                    else
                        throw new Error ( "Nothing to put, stats=" + stats + " / size=" + ( stats && stats.size ) );
                });
            });
        };

        this.unlink = function ()
        {
            delObject ( S3, bucket, file );
            this.end ();
        };

        this.forEach = function ( onData, onDone )
        {
            getObject ( S3, bucket, file, function ( err, data )
            {
                if ( err )
                    throw err;

                var line;

                data = data.Body.toString ().split ( /\s*\n\s*/ );
                while (( line = data.shift () ))
                    if ( line )
                        onData ( line );

                onDone ();
            });
        };

        this.toString = function ()
        {
            return '[S3File ' + bucket + ':' + file + ']';
        };
    };



        ////    File operations.

    return {

        list : function ( bucket, name, callback )
        {
            if ( !bucket || !/^[a-zA-Z0-9_.-]+$/.test ( bucket ) )
                throw new Error ( "Bad bucket : " + bucket );
            if ( !name || !/^[a-zA-Z0-9]+$/.test ( name ) )
                throw new Error ( "Bad filename : " + name );

            listObjects ( S3, bucket, name + '.', function ( err, filenames )
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

                    file = new File ( bucket, filename );
                    file.rev = matches [ 1 ];
                    files.push ( file );
                });

                files.sort ( function ( a, b )
                {
                    return a.rev < b.rev ? -1 : a.rev > b.rev ? 1 : 0;
                });

                callback ( null, files );
            });
        },

        make : function ( bucket, name, rev )
        {
            if ( !bucket || !/^[a-zA-Z0-9_.-]+$/.test ( bucket ) )
                throw new Error ( "Bad bucket : " + bucket );
            if ( !name || !/^[a-zA-Z0-9]+$/.test ( name ) )
                throw new Error ( "Bad filename : " + name );

            if ( !rev )
                rev = '0';

            var file = new File ( bucket, name + '.' + rev );
                file.rev = String ( rev );

            return file;
        }
    };
};



    ////    awssum helpers.

function listObjects ( S3, bucket, prefix, callback )
{
    var objects = [],
        options, onData, next;

    options =
    {
        BucketName  : bucket,
        MaxKeys     : 1000,
        Prefix      : prefix
    };

    onData = function ( err, data )
    {
        console.log ( "DIVAN S3> ListObjects response", err, data );
        if ( err )
            callback ( err );

        else
        {
            if ( data.Body.ListBucketResult.Contents )
                data.Body.ListBucketResult.Contents.forEach ( function ( entry )
                {
                    objects.push ( entry.Key );
                });

            if ( data.Body.ListBucketResult.IsTruncated === 'true' )
            {
                options.Marker = objects [ objects.length ];
                next ();
            }

            else
                callback ( null, objects );
        }
    };

    next = function ()
    {
        S3.ListObjects ( options, onData );
    };

    next ();
}

function putObject ( S3, bucket, object, size, body, callback )
{
    var options =
    {
        BucketName    : bucket,
        ObjectName    : object,
        ContentLength : size,
        Body          : body
    };

    console.log ( "DIVAN S3> Putting " + size + " bytes as " + object + " ..." );

    S3.PutObject ( options, function ( err, data )
    {
        console.log ( "DIVAN S3> PutObject response", err, data );
        callback ( err, data );
    });
}

function getObject ( S3, bucket, object, callback )
{
    var options =
    {
        BucketName : bucket,
        ObjectName : object
    };

    S3.GetObject ( options, callback );
}

function delObject ( S3, bucket, object )
{
    var options =
    {
        BucketName : bucket,
        ObjectName : object
    };

    S3.DeleteObject ( options, function ( err, data )
    {
        console.log ( "DIVAN S3> DeleteObject response", err, data );
        if ( err )
            throw err;
    });
}



