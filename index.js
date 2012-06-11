


////////    Quick configs.

var sugar = {};

    ////    Make a divan with a local append-only and snapshot files.

exports.cwd = function ( ns )
{
    return exports.makeDivan
    ({
        snapshot    : exports.makeLocalSnapshot ({ dir : './', name : ns, compact : true }),
        aof         : exports.makeLocalAOF ({ dir : './', name : ns, compact : true }),
        verbose     : !!exports.debug
    });
};

    ////    Make a divan with a local AOF and snapshots on Amazon S3.

exports.s3 = function ( key, secret, region, bucket )
{
    if ( !bucket )
    {
        bucket = region;
        region = "us-east-1";
    }

    return exports.makeDivan
    ({
        snapshot            : exports.makeS3Snaphot
        ({
            key             : key,
            secret          : secret,
            region          : region,
            bucket          : bucket,
            compact         : true
        }),

        aof                 : exports.makeLocalAOF ({ dir : './', name : 's3.' + bucket, compact : true }),
        verbose             : !!exports.debug,
        snapshotInterval    : 3600 * 1000
    });
};



////////    Sugary extensions for divan instances' public API.

    ////    Add all views from a design directory.

sugar.design = function ( dir )
{
    var self  = this,
        views = exports.readDesignDir ({ dir : dir });

    Object.keys ( views ).forEach ( function ( name )
    {
        self.addView ( name, views [ name ] );
    });

    return self;
};



////////    Helpers.

    ////    Make a map/reduce view from two functions.

exports.mr = function ( mapper, reducer )
{
    return new ( require ( "./lib/mrview" ) )
    (
        mapper,
        reducer,
        new ( require ( "./lib/mindex" ) ),
        new ( require ( "./lib/rcache" ) )
    );
};



////////    Components for advanced configs.

exports.makeDivan = function ( opts )
{
    validateOptions ( opts );
    var db = new ( require ( "./lib/divan" ) ) ( opts );

    Object.keys ( sugar ).forEach ( function ( prop )
    {
        db [ prop ] = sugar [ prop ];
    });

    return db;
};

    ////    .aof options

exports.makeLocalAOF = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/aof" ) ) ( opts, require ( "./lib/file" ) );
};

exports.makeRedisAOF = function ( opts )
{
    var client = opts.client;
    opts.dir = opts.bucket;
    validateOptions ( opts );

    if ( !client ) client = require ( "redis" ).createClient ( opts.port, opts.host, opts );
    if ( opts.pass ) client.AUTH ( opts.pass );

    return new ( require ( "./lib/aof" ) ) ( opts, require ( "./lib/redfile" ) ( client ) );
};

    ////    .snapshot options

exports.makeLocalSnapshot = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/snap" ) ) ( opts, require ( "./lib/file" ) );
};

exports.makeS3Snaphot = function ( opts )
{
    opts.dir = opts.bucket;
    validateOptions ( opts );
    return new ( require ( "./lib/snap" ) )
    (
        opts,
        require ( "./lib/s3file" )
        (
            new ( require ( 'awssum' ).load ( 'amazon/s3' ).S3 )
            ({
                accessKeyId : opts.key,
                secretAccessKey : opts.secret,
                region : opts.region
            })
        )
    );
};

    ////    .views options

exports.readDesignDir = function ( opts )
{
    validateOptions ( opts );
    return require ( "./lib/mrutils" ).readDesignDir ( opts.dir, exports.mr );
};



////////    Throw errors for bad options to help debug.

function validateOptions ( opts )
{
    if ( !opts )
        throw new Error ( "Undefined options object." );

    var key;
    if ( opts ) for ( key in opts )
        if ( opts.hasOwnProperty ( key ) && typeof opts [ key ] === 'undefined' )
            throw new Error ( "Undefined constructor option : " + key );
}



