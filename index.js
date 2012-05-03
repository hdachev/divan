


////////    Quick configs.

/**
    .makeLocal :

    Make a divan with local compacted append-only and snapshot files,
        namespace works in the same way it does for `dirty`.

    @param namespace, string
        the filename prefix for the db's snapshot and append-only files,
        both will be put in the current working directory.

    @designdir designdir, string
        optional path of a directory containing .json and/or .js
        map/reduce view definitions.
 **/

exports.makeLocal = function ( namespace, designdir )
{
    return exports.makeDivan
    ({
        snapshot    : exports.makeLocalSnapshot ({ dir : './', name : namespace + '-snap', compact : true }),
        aof         : exports.makeLocalAOF ({ dir : './', name : namespace + '-aof', compact : true }),
        views       : ( designdir && exports.readDesignDir ({ dir : designdir }) ) || null
    });
};



////////    Components.

    ////    Divan factory.

exports.makeDivan = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/divan" ) ) ( opts );
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
    return require ( "./lib/mrutils" ).readDesignDir ( opts.dir, require ( "./lib/mrnaive" ) );
};



    ////

function validateOptions ( opts, obj )
{
    var key;
    if ( opts ) for ( key in opts )
        if ( opts.hasOwnProperty ( key ) && typeof opts [ key ] === 'undefined' )
            throw new Error ( "Undefined constructor option : " + key );
}



