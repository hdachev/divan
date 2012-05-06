

var sugar = {};

////////    Quick configs.

/**
    .cwd :

    Make a divan with local compacted append-only and snapshot files,
        namespace works in the same way it does for `dirty`.

    @param namespace, string
        the filename prefix for the db's snapshot and append-only files,
        both will be put in the current working directory.
 **/

exports.cwd = function ( ns )
{
    return exports.makeDivan
    ({
        snapshot    : exports.makeLocalSnapshot ({ dir : './', name : ns, compact : true }),
        aof         : exports.makeLocalAOF ({ dir : './', name : ns, compact : true }),
        verbose     : true
    });
};

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



////////    Components.

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
            new ( require ( 'awssum' ).load ( 'amazon/s3' ) )
                ( opts.key, opts.secret, opts.account, opts.region )
        )
    );
};

    ////    .views options

exports.readDesignDir = function ( opts )
{
    validateOptions ( opts );
    return require ( "./lib/mrutils" ).readDesignDir ( opts.dir, exports.mr );
};

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



////////    Utils.

function validateOptions ( opts, obj )
{
    var key;
    if ( opts ) for ( key in opts )
        if ( opts.hasOwnProperty ( key ) && typeof opts [ key ] === 'undefined' )
            throw new Error ( "Undefined constructor option : " + key );
}



