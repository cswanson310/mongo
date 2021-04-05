
t = db.agg_abt_basic_unwind1;
t.drop();

assert.commandWorked(t.insert({_id: 1}));
assert.commandWorked(t.insert({_id: 2, x: null}));
assert.commandWorked(t.insert({_id: 3, x: []}));
assert.commandWorked(t.insert({_id: 4, x: [1, 2]}));
assert.commandWorked(t.insert({_id: 5, x: [10]}));
assert.commandWorked(t.insert({_id: 6, x: 4}));

res = t.aggregate([{$unwind: '$x'}, {$sort: {'x': 1}}]).toArray();
assert.eq(4, res.length);
