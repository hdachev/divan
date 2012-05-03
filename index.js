


    ////    DB.

exports.makeDivan = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/divan" ) ) ( opts );
};



    ////    AOF options.

exports.makeLocalAOF = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/aof" ) ) ( opts, require ( "./lib/file" ) );
};



    ////    Snapshot options.

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



    ////    View servers.

exports.readDesignDir = function ( opts )
{
    validateOptions ( opts );
    return require ( "./lib/mrutils" ).readDesignDir ( opts.dir, require ( "./lib/mrnaive" ) );
};



    ////    Utils.

function validateOptions ( opts, obj )
{
    var key;
    if ( opts ) for ( key in opts )
        if ( opts.hasOwnProperty ( key ) && typeof opts [ key ] === 'undefined' )
            throw new Error ( "Undefined constructor option : " + key );
}



    ////    Quick configs.

exports.makeDirty = function ( name )
{
    return exports.makeDivan
    ({
        snapshot    : exports.makeLocalSnapshot ({ dir : './', name : name + '-snap', compact : true }),
        aof         : exports.makeLocalAOF ({ dir : './', name : name + '-aof', compact : true })
    });
};



