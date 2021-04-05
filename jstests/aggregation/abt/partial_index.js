
t = db.agg_abt_partial_index;
t.drop();

assert.commandWorked(t.insert({a: 1, b: 1, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 2, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 3, c: 1}));
assert.commandWorked(t.insert({a: 3, b: 3, c: 2}));
assert.commandWorked(t.insert({a: 4, b: 3, c: 2}));
assert.commandWorked(t.insert({a: 5, b: 5, c: 2}));

assert.commandWorked(t.createIndex({'a': 1}, {partialFilterExpression: {'b': 2}}));
// assert.commandWorked(t.createIndex({'a': 1}));

// Should have an index scan.
res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': 3, 'b': 2}}]).toArray();
assert.eq(1, res.length);

// Should not have an index scan.
res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a': 3, 'b': 3}}]).toArray();
assert.eq(2, res.length);
