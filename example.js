
//  Make a divan with local compacted append-only and snapshot files,
//  namespace works in the same way it does for `dirty`:

var divan = require ( './index' ),
    db = divan.cwd ( 'friends' );

//  Save some docs, generating your own id-s:

db.save ({ _id : 'don1', type : 'person', name : 'Don', gender : 'male' });
db.save ({ _id : 'samantha', type : 'person', name : 'Sam', gender : 'female' });
db.save ({ _id : 'i.v.a.n', type : 'person', name : 'Ivan', gender : 'male' });

//  These will be flushed to the AOF and then later compacted as a db snapshot.

//  Now register a view:

db.addView
(
    'gender/count',
    divan.mr
    (
        function ( doc, emit )
        {
            if ( doc.type === 'person' )
                emit ( doc.gender, 1 );
        },
        function ( k, v )
        {
            var i, n = v.length, sum = 0;
            for ( i = 0; i < n; i ++ ) sum += v [ i ];
            return sum;
        }
    )
);

//  Query the view:

db.view ( 'gender/count', { group : true }, function ( err, data )
{
    data.rows.forEach ( function ( row )
    {
        console.log ( row.key, row.value );
    });
});

//  Outputs:
//  female 1
//  male 2
