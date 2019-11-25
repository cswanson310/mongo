(function() {
"use strict";
const st = new ShardingTest({mongos: 6, shards: 3, config: 3, enableBalancer: true});

const coll = st.s0.getDB("test").remove_shard_agg;
const allMongoses = [st.s0, st.s1, st.s2, st.s3, st.s4, st.s5];
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 2000; ++i) {
    bulk.insert({region: i % 2 == 0 ? "US" : "EU", _id: i});
}
assert.commandWorked(bulk.execute());
assert.commandWorked(coll.ensureIndex({region: 1, _id: 1}));
assert(st.enableSharding("test"));
st.ensurePrimaryShard("test", st.shard0.shardName);
st.shardColl(coll, {region: 1, _id: 1}, false);
assert(st.adminCommand({split: coll.getFullName(), middle: {region: "US", _id: MinKey}}));
for (let i = 1; i < 11; i++) {
    assert(st.adminCommand({split: coll.getFullName(), middle: {region: "US", _id: 10 * i}}));
}
assert(st.adminCommand({split: coll.getFullName(), middle: {region: "EU", _id: MinKey}}));
for (let i = 1; i < 11; i++) {
    assert(st.adminCommand({split: coll.getFullName(), middle: {region: "EU", _id: 10 * i}}));
}
assert(st.adminCommand({addShardToZone: st.shard0.shardName, zone: "EU"}));
assert(st.adminCommand({addShardToZone: st.shard1.shardName, zone: "US"}));
assert(st.adminCommand({addShardToZone: st.shard2.shardName, zone: "US"}));
assert(st.adminCommand({
    updateZoneKeyRange: coll.getFullName(),
    min: {region: MinKey, _id: MinKey},
    max: {region: "US", _id: MinKey},
    zone: "EU"
}));
assert(st.adminCommand({
    updateZoneKeyRange: coll.getFullName(),
    min: {region: "US", _id: MinKey},
    max: {region: MaxKey, _id: MaxKey},
    zone: "US"
}));
st.printShardingStatus();

let aggThreads = [];
for (let mongos of allMongoses) {
    aggThreads.push(startParallelShell(() => {
        while (db.SENTINEL.findOne() == null) {
            // db.something_else.find().itcount();
            let x = db.remove_shard_agg.aggregate([{$count: "total"}]).toArray()[0];
            // sleep(50);
        }
    }, mongos.port));
}

function flushConfigs() {
    for (let mongos of allMongoses) {
        assert.commandWorked(mongos.adminCommand({flushRouterConfig: "test"}));
    }
    for (let shard of [st.rs0.getPrimary(), st.rs1.getPrimary(), st.rs2.getPrimary()]) {
        assert.commandWorked(shard.adminCommand({flushRouterConfig: "test"}));
    }
}
const shard2 = st.rs2;
let removeStatus;
let mongosCounter = 0;
assert.soon(
    function() {
        removeStatus =
            assert.commandWorked(st.getDB("admin").runCommand({removeShard: st.shard2.shardName}));
        if (removeStatus.hasOwnProperty("remaining") &&
            removeStatus.remaining.hasOwnProperty("chunks") && removeStatus.remaining.chunks == 0 &&
            removeStatus.hasOwnProperty("dbsToMove") && removeStatus.dbsToMove.length > 0) {
            // There are no chunks left to move but we still have to move the database.
            for (let db of removeStatus.dbsToMove) {
                st.ensurePrimaryShard(db, st.shard0.shardName);
                flushConfigs();
            }
        }
        return removeStatus.state === "completed";
    },
    () => `Shard removal timed out, most recent removeShard response: ${tojson(removeStatus)}`,
    60000);
removeStatus = assert.commandWorkedOrFailedWithCode(
    st.getDB("admin").runCommand({removeShard: st.shard2.shardName}), ErrorCodes.ShardNotFound);
shard2.stopSet();
// flushConfigs();
const rst = new ReplSetTest({
    nodes: 1,
});
rst.startSet({shardsvr: ''});
rst.initiate();
assert.commandWorked(st.s.adminCommand({addShard: rst.getURL(), name: "newShard"}));
assert(st.adminCommand({addShardToZone: "newShard", zone: "EU"}));
st.printShardingStatus();
const shard0 = st.rs0;
assert.soon(
    function() {
        removeStatus =
            assert.commandWorked(st.getDB("admin").runCommand({removeShard: st.shard0.shardName}));
        if (removeStatus.hasOwnProperty("remaining") &&
            removeStatus.remaining.hasOwnProperty("chunks") && removeStatus.remaining.chunks == 0 &&
            removeStatus.hasOwnProperty("dbsToMove") && removeStatus.dbsToMove.length > 0) {
            // There are no chunks left to move but we still have to move the database.
            for (let db of removeStatus.dbsToMove) {
                st.ensurePrimaryShard(db, "newShard");
            }
            // flushConfigs();
        }
        return removeStatus.state === "completed";
    },
    () => `Shard removal timed out, most recent removeShard response: ${tojson(removeStatus)}`,
    60000);
flushConfigs();

shard0.stopSet();
assert.commandWorked(st.s.getDB("test").SENTINEL.insertOne({_id: "test over"}));
for (let threadJoiner of aggThreads) {
    threadJoiner();
}

rst.stopSet();
st.stop();
}());
