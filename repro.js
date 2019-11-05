(function() {
"use strict";

const st = new ShardingTest({shards: 2, config: 1});

const mongosDB = st.s.getDB("test");
const source = mongosDB.source;
const target = mongosDB.target;

st.shardColl(source, {_id: 1}, {_id: 1}, {_id: 2});

const bulk = source.initializeUnorderedBulkOp();
for (let i = 0; i < 20000; i++) {
    bulk.insert({_id: i, x: [i - 1, i, i + 1]});
}
assert.commandWorked(bulk.execute());

const start = new Date();
source.aggregate([
    {$unwind: "$x"},
    {$sort: {_id: -1}},
    {$sort: {_id: 1}},
    {$sort: {_id: -1}},
    {$merge: {into: target.getName()}}
]);
jsTestLog("Elapsed: " + ((new Date() - start) / 1000) + "s");
assert.eq(target.find().itcount(), 20000);

st.stop();
}());
