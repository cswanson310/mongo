
t = db.agg_abt_index_intersect1;
t.drop();

assert.commandWorked(t.insert({a: 50}));
assert.commandWorked(t.insert({a: 70}));
assert.commandWorked(t.insert({a: 90}));
assert.commandWorked(t.insert({a: 110}));
assert.commandWorked(t.insert({a: 130}));

assert.commandWorked(t.createIndex({'a': 1}));

// Add a stage with a hint to inhibit optimization since without it the query gets rewritten to
// a find query.
res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': {$gt: 60, $lt: 100}}}])
          .toArray();
assert.eq(2, res.length);
