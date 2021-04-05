
t = db.agg_abt_index_intersect;
t.drop();

assert.commandWorked(t.insert({a: 1, b: 1, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 2, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 3, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 3, c: 2}));
assert.commandWorked(t.insert({a: 4, b: 3, c: 2}));
assert.commandWorked(t.insert({a: 5, b: 5, c: 2}));

assert.commandWorked(t.createIndex({'a': 1}));
assert.commandWorked(t.createIndex({'b': 1}));

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': 3, 'b': 3}}]).toArray();
printjson(res);
assert.eq(2, res.length);
