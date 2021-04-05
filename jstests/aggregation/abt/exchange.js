assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryDefaultDOP: 5}));

t = db.agg_abt_exchange;
t.drop();

assert.commandWorked(t.insert({a: {b: 1}}));
assert.commandWorked(t.insert({a: {b: 2}}));
assert.commandWorked(t.insert({a: {b: 3}}));
assert.commandWorked(t.insert({a: {b: 4}}));
assert.commandWorked(t.insert({a: {b: 5}}));

res = t.aggregate([{$_internalInhibitOptimization: {}}, {$match: {'a.b': 2}}]).toArray();
assert.eq(1, res.length);
