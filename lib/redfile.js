

    ////    File wrapper for a node_redis client, meant for append-only files.
    ////    Uses strings for file bodies and sets for directory listings.

    ////    You can use this perhaps together with the S3 driver
    ////        to ensure you don't lose data in between snapshots.

var UNCOMMITED = '.uncommitted';

module.exports = function ( client )
{

    var File = function ( bucket, file )
    {
        var key = bucket + '-' + file,
            writer;


        this.atomic = function ()
        {
            if ( writer )
                throw new Error ( "Already writing." );

            writer = { write : client.APPEND.bind ( client, key ) };
            key += UNCOMMITED;

            client.SADD ( bucket + '+' + UNCOMMITED, file );
        };

        this.append = function ( line )
        {
            if ( !writer )
            {
                writer = { write : client.APPEND.bind ( client, key ) };
                client.SADD ( bucket, file );
            }

            writer.write ( line + '\n' );
        };

        this.commit = function ( errback )
        {
            var key0 = key,
                key1 = key.replace ( UNCOMMITED, '' );

            if ( key0 === key1 )
                throw new Error ( "Not atomic." );

            writer = { write : function () { throw new Error ( "This file is closed." ); } };
            key    = key1;

            client.SADD ( bucket, file );
            client.RENAME ( key0, key1 );
            client.SREM ( bucket + '+' + UNCOMMITED, file );
            client.STRLEN ( key, errback );
        };


        this.unlink = function ()
        {
            writer = { write : function () { throw new Error ( "This file is unlinked." ); } };
            client.DEL ( key );
            client.SREM ( bucket, file );
        };


        this.forEach = function ( onData, onDone )
        {
            client.GET ( key, function ( err, str )
            {
                if ( err )
                    throw err;

                var line,
                    data = str.split ( /\s*\n\s*/ );

                while (( line = data.shift () ))
                    if ( line )
                        onData ( line );

                onDone ();
            });
        };

        this.toString = function ()
        {
            return '[RedisFile ' + bucket + '/' + file + ']';
        };

    };



        ////    File operations.

    return {

        list : function ( bucket, name, ext, callback )
        {
            if ( !bucket || !/^[a-zA-Z0-9_.-]+$/.test ( bucket ) )
                throw new Error ( "Bad bucket : " + bucket );
            if ( !name || !/^[a-zA-Z0-9_.-]+$/.test ( name ) )
                throw new Error ( "Bad filename : " + name );
            if ( !ext || !/^[a-zA-Z0-9]+$/.test ( ext ) )
                throw new Error ( "Bad extension : " + name );

            bucket += '.' + ext;

            client.SMEMBERS ( bucket, function ( err, filenames )
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

        make : function ( bucket, name, ext, rev )
        {
            if ( !bucket || !/^[a-zA-Z0-9_.-]+$/.test ( bucket ) )
                throw new Error ( "Bad bucket : " + bucket );
            if ( !name || !/^[a-zA-Z0-9_.-]+$/.test ( name ) )
                throw new Error ( "Bad filename : " + name );
            if ( !ext || !/^[a-zA-Z0-9]+$/.test ( ext ) )
                throw new Error ( "Bad extension : " + name );

            if ( !rev ) rev = '0';
            bucket += '.' + ext;

            var file = new File ( bucket, name + '.' + rev );
                file.rev = String ( rev );

            return file;
        }
    };
};




