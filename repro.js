(function() {
"use strict";
const st = new ShardingTest({mongos: 6, shards: 3, config: 3, enableBalancer: true});

const coll = st.s0.getDB("test").remove_shard_agg;
const allMongoses = [st.s0, st.s1, st.s2, st.s3, st.s4, st.s5];
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 200; ++i) {
    bulk.insert({region: i % 2 == 0 ? "US" : "EU", _id: i});
}
assert.commandWorked(bulk.execute());
assert.commandWorked(coll.ensureIndex({region: 1, _id: 1}));
assert(st.enableSharding("test"));
st.ensurePrimaryShard("test", st.shard0.shardName);
st.shardColl(coll, {region: 1, _id: 1}, false);
assert(st.adminCommand({split: coll.getFullName(), middle: {region: "US", _id: MinKey}}));
assert(st.adminCommand({split: coll.getFullName(), middle: {region: "US", _id: 100}}));
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

let removeStatus;
assert.soon(function() {
    removeStatus =
        assert.commandWorked(st.getDB("admin").runCommand({removeShard: st.shard2.shardName}));
    return removeStatus.state === "completed";
}, () => `Shard removal timed out, most recent removeShard response: ${tojson(removeStatus)}`);
for (let mongos of allMongoses) {
    jsTestLog(
        mongos.getCollection("test.remove_shard_agg").aggregate([{$count: "total"}]).toArray()[0]);
}
const rst = new ReplSetTest({
    nodes: 1,
});
rst.startSet({shardsvr: ''});
rst.initiate();
assert.commandWorked(st.s.adminCommand({addShard: rst.getURL(), name: "newShard"}));
assert(st.adminCommand({addShardToZone: "newShard", zone: "EU"}));
st.printShardingStatus();
for (let i = 0; i < 100; ++i) {
    for (let mongos of allMongoses) {
        jsTestLog(mongos.getCollection("test.remove_shard_agg")
                      .aggregate([{$count: "total"}])
                      .toArray()[0]);
    }
    sleep(50);
}

rst.stopSet();
st.stop();
}());
