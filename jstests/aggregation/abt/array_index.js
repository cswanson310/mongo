
t = db.agg_abt_array_index;
t.drop();

assert.commandWorked(t.insert({a: [1, 2, 3, 4]}));
assert.commandWorked(t.insert({a: [2, 3, 4]}));
assert.commandWorked(t.insert({a: [2]}));
assert.commandWorked(t.insert({a: 2}));
assert.commandWorked(t.insert({a: [1, 3]}));

assert.commandWorked(t.createIndex({'a': 1}));

// Add a stage with a hint to inhibit optimization since without it the query gets rewritten to a
// find query.
res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': 2}}]).toArray();
// printjson(res);
assert.eq(4, res.length);

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': {$lt: 2}}}]).toArray();
// printjson(res);
assert.eq(2, res.length);
