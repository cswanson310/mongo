/**
 * TODO: Description.
 * @tags: [requires_sharding]
 */

(function() {
"use strict";
load("jstests/aggregation/extras/utils.js");  // arrayEq
const st = new ShardingTest({name: jsTestName(), shards: 2, mongos: 1, config: 1});

const testDB = st.s.getDB(jsTestName());
const collA = testDB.A;
const collB = testDB.B;
const collC = testDB.C;
const collD = testDB.D;
const collArray = [collA, collB, collC, collD];
// Create four collections with indexes, then shard on those indexes.
collA.createIndex({a: 1});
collB.createIndex({b: 1});
collC.createIndex({c: 1});
collD.createIndex({d: 1});
testDB.adminCommand({enableSharding: testDB.getName()});
testDB.adminCommand({shardCollection: collA.getFullName(), key: {a: 1}});
testDB.adminCommand({shardCollection: collB.getFullName(), key: {b: 1}});
testDB.adminCommand({shardCollection: collC.getFullName(), key: {c: 1}});
testDB.adminCommand({shardCollection: collD.getFullName(), key: {d: 1}});
// Insert five documents into each collection.
for (var i = 0; i < 5; i++) {
    assert.commandWorked(collA.insert({a: i, val: i, secondary: i}));
}
for (var i = 0; i < 5; i++) {
    assert.commandWorked(collB.insert({b: i, val: i * 2, secondary: i}));
}
for (var i = 0; i < 5; i++) {
    assert.commandWorked(collC.insert({c: i, val: 10 - i, secondary: i}));
}
for (var i = 0; i < 5; i++) {
    assert.commandWorked(collD.insert({d: i, val: i * 3, secondary: i}));
}

// Split each collection into two chunks, and make sure there is one chunk on each shard.
testDB.adminCommand({split: collA.getFullName(), middle: {a: 2}});
testDB.adminCommand({moveChunk: collA.getFullName(), find: {a: 1}, to: st.shard0.shardName});
testDB.adminCommand({moveChunk: collA.getFullName(), find: {a: 3}, to: st.shard1.shardName});
testDB.adminCommand({split: collB.getFullName(), middle: {b: 2}});
testDB.adminCommand({moveChunk: collB.getFullName(), find: {b: 1}, to: st.shard0.shardName});
testDB.adminCommand({moveChunk: collB.getFullName(), find: {b: 3}, to: st.shard1.shardName});
testDB.adminCommand({split: collC.getFullName(), middle: {c: 2}});
testDB.adminCommand({moveChunk: collC.getFullName(), find: {c: 1}, to: st.shard0.shardName});
testDB.adminCommand({moveChunk: collC.getFullName(), find: {c: 3}, to: st.shard1.shardName});
testDB.adminCommand({split: collD.getFullName(), middle: {d: 2}});
testDB.adminCommand({moveChunk: collD.getFullName(), find: {d: 1}, to: st.shard0.shardName});
testDB.adminCommand({moveChunk: collD.getFullName(), find: {d: 3}, to: st.shard1.shardName});

function runTest(runAgainstDB, aggregation, expectedResult) {
    const resObj = assert.commandWorked(runAgainstDB.runCommand(aggregation));
    const res = resObj.cursor.firstBatch;
    assert(arrayEq(res, expectedResult),
           "Expected:\n" + tojson(expectedResult) + "Got:\n" + tojson(res))
}

function getDocsFromCollection(collObj, proj = null) {
    if (proj == null) {
        return collObj.find({}, proj).toArray();
    } else {
        return collObj.aggregate({"$addFields": proj}).toArray();
    }
}

// Test a two collection union.
let resSet = getDocsFromCollection(collA).concat(getDocsFromCollection(collB));
runTest(testDB,
        {aggregate: collA.getName(), pipeline: [{"$unionWith": collB.getName()}], cursor: {}},
        resSet)
// Test a sequential four collection union.
resSet = getDocsFromCollection(collA).concat(
    getDocsFromCollection(collB), getDocsFromCollection(collC), getDocsFromCollection(collD));
runTest(testDB,
        {
            aggregate: collA.getName(),
            pipeline: [
                {"$unionWith": collB.getName()},
                {"$unionWith": collC.getName()},
                {"$unionWith": collD.getName()}
            ],
            cursor: {}
        },
        resSet)
// Test a nested four collection union.
// resSet should be the same.
runTest(testDB,
        {
            aggregate: collA.getName(),
            pipeline: [{
                "$unionWith": {
                    coll: collB.getName(),
                    pipeline: [{
                        "$unionWith":
                            {coll: collC.getName(), pipeline: [{"$unionWith": collD.getName()}]}
                    }]
                }
            }],
            cursor: {}
        },
        resSet)
// Test that a sub-pipeline is applied to the correct documents.
resSet = getDocsFromCollection(collA).concat(getDocsFromCollection(collB, {x: 3}));
runTest(testDB,
        {
            aggregate: collA.getName(),
            pipeline: [{"$unionWith": {coll: collB.getName(), pipeline: [{"$addFields": {x: 3}}]}}],
            cursor: {}
        },
        resSet)
// Test that for multiple nested unions sub-pipelines are applied to the correct documents.
resSet = getDocsFromCollection(collA).concat(getDocsFromCollection(collB, {x: 3}),
                                             getDocsFromCollection(collC, {x: 3, y: 4}),
                                             getDocsFromCollection(collD, {x: 3, y: 4, z: 5}));
runTest(testDB,
        {
            aggregate: collA.getName(),
            pipeline: [{
                "$unionWith": {
                    coll: collB.getName(),
                    pipeline: [
                        {
                            "$unionWith": {
                                coll: collC.getName(),
                                pipeline: [
                                    {
                                        "$unionWith": {
                                            coll: collD.getName(),
                                            pipeline: [{"$addFields": {z: 5}}]
                                        }
                                    },
                                    {"$addFields": {y: 4}}
                                ]
                            }
                        },
                        {"$addFields": {x: 3}}
                    ]
                }
            }],
            cursor: {}
        },
        resSet)
resSet = getDocsFromCollection(collA).concat(getDocsFromCollection(collB, {x: 3}),
                                             getDocsFromCollection(collC, {x: 3, y: 4}),
                                             getDocsFromCollection(collD, {x: 3, z: 5}));
runTest(testDB,
        {
            aggregate: collA.getName(),
            pipeline: [{
                "$unionWith": {
                    coll: collB.getName(),
                    pipeline: [
                        {
                            "$unionWith": {
                                coll: collC.getName(),
                                pipeline: [
                                    {"$addFields": {y: 4}}
                                ]
                            }
                        },
                        {
                            "$unionWith": {
                                coll: collD.getName(),
                                pipeline: [{"$addFields": {z: 5}}]
                            }
                        },
                        {"$addFields": {x: 3}}
                    ]
                }
            }],
            cursor: {}
        },
        resSet)
// Test with $group.
resSet = [{_id: 0, sum: 0}, {_id: 1, sum: 3}, {_id: 2, sum: 6}, {_id: 3, sum: 9}, {_id: 4, sum: 12}];
runTest(testDB,{
    aggregate: collA.getName(),
    pipeline: [{"$unionWith": collB.getName()}, {"$group": {_id: "$secondary", sum: {$sum: "$value"}}}],
    cursor: {}
}, resSet);
st.stop();
})();
