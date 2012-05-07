

Fast **in-memory**, **in-process** key-value store for node
with **snapshot and AOF persistance**
and **CouchDB-style map-reduce views**.
Just make sure your data fits in memory,
currently I wouldn't recommend divan
for a dataset with more than 500K docs.


### why? because!

- some tasks and workloads just don't deserve their own Couch but can benefit from a similar data-model.
- not a Couch - which among other things means it doesn't do MVCC, so delete and update as much as you want.
- super fast, when fully warmed up serves thousands of queries per second, and because it runs in-process, there's no network latency.


### usage

    npm install divan

**index.js**

```javascript

//  Make a divan with local compacted append-only and snapshot files,
//  namespace works in the same way it does for `dirty`:

var divan = require ( 'divan' ),
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

```

You can add views via `db.addView`
or by parsing a directory of
design files via `db.design(path)`.
The design-files can either be
.json files of couchdb-design-doc flavour,
or .js files that export objects
with `map` and `reduce` methods.
Note that when using .js docs,
`map` functions need to accept
the `emit` function as the second parameter.


### CouchDB view API coverage

Everything but `group_level` and `include_docs`.


### lazy views and reduce caching

Instead of populating views immediately,
divan waits for your first query
before producing map result for a view.
This means that you can
have as many designs as you want,
if only use a few, the rest won't eat up memory.

Further, reduce results are only computed and cached
for the ranges of a view that you actually access.
Once warmed up, the caches are invalidated and rebuild
very quickly on writes and deletes.

Brief, if you want to fully warm up a reduce view,
query it with `group=true` or `group=false`
depending on whether you'll ever use ungrouped results,
and without specifying a key-range.


### what else

- You can iterate your entire db with `db.forEach(func)`
- If you look at the sources, you'll see that there's an option to have your snapshots on Amazon S3.
- By using `db.addView("view-name", ["source-view", "other-source-view"], viewObj)` you can do chained map/reduce.


