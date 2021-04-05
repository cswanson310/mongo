
t = db.agg_abt_basic_unwind;
t.drop();

assert.commandWorked(t.insert({_id: 1}));
assert.commandWorked(t.insert({_id: 2, x: null}));
assert.commandWorked(t.insert({_id: 3, x: []}));
assert.commandWorked(t.insert({_id: 4, x: [1, 2]}));
assert.commandWorked(t.insert({_id: 5, x: [3]}));
assert.commandWorked(t.insert({_id: 6, x: 4}));

res = t.aggregate([{$unwind: '$x'}]).toArray();
assert.eq(4, res.length);

t = db.agg_abt_basic_index;
t.drop();

assert.commandWorked(t.insert({a: {b: 1}}));
assert.commandWorked(t.insert({a: {b: 2}}));
assert.commandWorked(t.insert({a: {b: 3}}));
assert.commandWorked(t.insert({a: {b: 4}}));
assert.commandWorked(t.insert({a: {b: 5}}));

assert.commandWorked(t.createIndex({'a.b': 1}));

// Add a stage with a hint to inhibit optimization since without it the query gets rewritten to a
// find query.
res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': 2}}]).toArray();
assert.eq(1, res.length);

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': {$gt: 2}}}]).toArray();
assert.eq(3, res.length);

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': {$gte: 2}}}]).toArray();
assert.eq(4, res.length);

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': {$lt: 2}}}]).toArray();
assert.eq(1, res.length);

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': {$lte: 2}}}]).toArray();
assert.eq(2, res.length);
