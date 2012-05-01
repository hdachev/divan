


    ////    DB.

exports.makeDivan = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/divan" ).Divan ) ( opts );
};



    ////    AOF options.

exports.makeLocalAOF = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/localaof" ).LocalAOF ) ( opts );
};



    ////    Snapshot options.

exports.makeLocalSnapshot = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/localsnap" ).LocalSnapshot ) ( opts );
};

exports.makeKnoxSnaphot = function ( opts )
{
    validateOptions ( opts );
    return new ( require ( "./lib/knoxsnap" ).KnoxSnaphot ) ( opts );
};



////////    utils.

function validateOptions ( opts, obj )
{
    var key;
    if ( opts )
        for ( key in opts )
            if ( opts.hasOwnProperty ( key ) && typeof opts [ key ] === 'undefined' )
                throw new Error ( "Undefined constructor option : " + key );
}


