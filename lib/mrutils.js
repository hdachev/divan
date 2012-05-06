

    ////    If underscore is available,
    ////        make it the global namespace for compiled map and reduce functions.

exports.builtIn = {};

try
{
    ( function ( _ )
    {
        exports.builtIn = _;

        if ( !_.sum ) _.sum = function ( arr )
        {
            return _.reduce ( arr, function ( a, b ) { return a + b; }, 0 );
        };

        if ( !_.avg ) _.avg = function ( arr )
        {
            return _.sum ( arr ) / arr.length;
        };

        if ( !_.count ) _.count = function ( arr )
        {
            return arr.length;
        };
    }
    ( require ( 'underscore' ) ) );
}
catch ( e ) {}


    ////    Compile couchdb-style map and reduce functions to something we can use.

exports.compileMapper = function ( source, name )
{
    var vm = require ( 'vm' ),
        context = vm.createContext ( exports.builtIn ),
        out;

    if ( !name )
        name = "compiled.map";

    context.emit = function ( k, v )
    {
        out ( k, v );
    };

    vm.runInNewContext ( '__map__ = ' + source, context, name );
    return function ( doc, onData )
    {
        out = onData;
        try
        {
            context.__map__ ( doc );
        }
        catch ( e )
        {
            console.log ( name, e );
        }
    };
};

exports.compileReducer = function ( source, name )
{
    var out;

    if ( /^_[a-z]+$/.test ( source ) )
    {
        out = exports.builtIn [ source.substr ( 1 ) ];
        if ( !out )
            throw new Error ( "'_" + out + "' is not built-in." );

        return function ( k, v )
        {
            return out ( v );
        };
    }

    var vm = require ( 'vm' ),
        context = vm.createContext ( exports.builtIn );

    if ( !name )
        name = "compiled.reduce";

    vm.runInNewContext ( '__reduce__ = ' + source, context, name );
    return function ( k, v )
    {
        try
        {
            return context.__reduce__ ( k, v, !k );
        }
        catch ( e )
        {
            console.log ( name, e );
        }
    };
};


    ////    Utilities for parsing js and json files with design documents.

exports.readDesignDoc = function ( doc, makeView )
{
    var views = {},
        object, name, m, r;

    if ( doc.views )
        for ( name in doc.views )
        {
            object = doc.views [ name ];
            m = null;
            r = null;

            if ( typeof object.map === 'function' )
                m = object.map;
            else if ( typeof object.map === 'string' )
                m = exports.compileMapper ( object.map, name + ".map" );
            else
                throw new Error ( "View must have a map function : " + name );

            if ( typeof object.reduce === 'function' )
                r = object.reduce;
            else if ( typeof object.reduce === 'string' )
                r = exports.compileReducer ( object.reduce, name + ".reduce" );

            views [ name ] = makeView ( m, r );
        }

    return views;
};

exports.readDesignDir = function ( dir, makeView )
{
    var fs = require ( 'fs' ), out = {};

    fs.readdirSync ( dir ).forEach ( function ( filename )
    {
        var matches,
            doc, name, views;

        if (( matches = /^([a-z0-9_.-]+)\.json$/i.exec ( filename ) ))
            doc = JSON.parse ( fs.readFileSync ( dir + '/' + filename ) );
        else if (( matches = /^([a-z0-9_.-]+)\.js$/i.exec ( filename ) ))
            doc = require ( dir + '/' + filename );

        else
        {
            console.log ( "DIVAN MRU.parseDesignDir> Ignoring " + filename + ", only .json and .js files supported ..." );
            return;
        }

        if ( doc )
        {
            views = exports.readDesignDoc ( doc, makeView );
            for ( name in views )
                if ( views.hasOwnProperty ( name ) )
                    out [ matches [ 1 ] + '/' + name ] = views [ name ];
        }
    });

    return out;
};


    

