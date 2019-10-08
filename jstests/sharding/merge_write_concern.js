// Tests that $merge respects the writeConcern set on the original aggregation command.
(function() {
"use strict";

load("jstests/aggregation/extras/merge_helpers.js");  // For withEachMergeMode.

const st = new ShardingTest({shards: 2, rs: {nodes: 3}, config: 1});

const mongosDB = st.s0.getDB("merge_write_concern");
const source = mongosDB["source"];
const target = mongosDB["target"];
const shard0 = st.rs0;
const shard1 = st.rs1;

// Enable sharding on the test DB and ensure its primary is shard0.
assert.commandWorked(mongosDB.adminCommand({enableSharding: mongosDB.getName()}));
st.ensurePrimaryShard(mongosDB.getName(), st.shard0.shardName);

function testWriteConcernError({rs, resetTargetFn}) {
    // Make sure that there are only 2 nodes up so w:3 writes will always time out.
    const stoppedSecondary = rs.getSecondary();
    rs.stop(stoppedSecondary);

    // Test that $merge correctly returns a WC error.
    withEachMergeMode(({whenMatchedMode, whenNotMatchedMode}) => {
        resetTargetFn();  // Be careful to avoid using remove({}) to work around SERVER-38852.
        const res = mongosDB.runCommand({
            aggregate: "source",
            pipeline: [{
                $merge: {
                    into: "target",
                    whenMatched: whenMatchedMode,
                    whenNotMatched: whenNotMatchedMode
                }
            }],
            writeConcern: {w: 3, wtimeout: 100},
            cursor: {},
        });

        // $merge writeConcern errors are handled differently from normal writeConcern
        // errors. Rather than returing ok:1 and a WriteConcernError, the entire operation
        // fails.
        assert.commandFailedWithCode(res,
                                     whenNotMatchedMode == "fail"
                                         ? [13113, ErrorCodes.WriteConcernFailed]
                                         : ErrorCodes.WriteConcernFailed);
    });

    // Restart the stopped node and verify that the $merge's now pass.
    rs.restart(rs.getSecondary());
    rs.awaitReplication();
    withEachMergeMode(({whenMatchedMode, whenNotMatchedMode}) => {
        // Skip the combination of merge modes which will fail depending on the contents of the
        // source and target collection, as this will cause the assertion below to trip.
        if (whenNotMatchedMode == "fail")
            return;

        resetTargetFn();  // Be careful to avoid using remove({}) to work around SERVER-38852.
        const res = mongosDB.runCommand({
            aggregate: "source",
            pipeline: [{
                $merge: {
                    into: "target",
                    whenMatched: whenMatchedMode,
                    whenNotMatched: whenNotMatchedMode
                }
            }],
            writeConcern: {w: 3},
            cursor: {},
        });

        // Ensure that the write concern is satisfied within a reasonable amount of time. This
        // prevents the test from hanging if for some reason the write concern can't be
        // satisfied.
        assert.soon(() => assert.commandWorked(res), "writeConcern was not satisfied");
    });
}

// Test that when both collections are unsharded, all writes are directed to the primary shard.
assert.commandWorked(source.insert([{_id: -1}, {_id: 0}, {_id: 1}, {_id: 2}]));
testWriteConcernError({rs: shard0, resetTargetFn: () => target.drop()});

// Shard the source collection and continue to expect writes to the primary shard.
st.shardColl(source, {_id: 1}, {_id: 0}, {_id: 1}, mongosDB.getName());
testWriteConcernError({rs: shard0, resetTargetFn: () => target.drop()});

// Test with the target collection sharded, however make sure that all writes go to the primary
// shard by splitting the collection at {_id: 10} and keeping all values in the same chunk.
function resetTargetToBeSharded() {
    target.drop();
    st.shardColl(target, {_id: 1}, {_id: 10}, {_id: 10}, mongosDB.getName());
    assert.eq(FixtureHelpers.isSharded(target), true);
}
testWriteConcernError({rs: shard0, resetTargetFn: resetTargetToBeSharded});

// Write a few documents to the source collection which will be $merge-ed to the second
// shard.
assert.commandWorked(source.insert([{_id: 11}, {_id: 12}, {_id: 13}]));

// Verify that either shard can produce a WriteConcernError since writes are going to both.
testWriteConcernError({rs: shard0, resetTargetFn: resetTargetToBeSharded});
testWriteConcernError({rs: shard0, resetTargetFn: resetTargetToBeSharded});

st.stop();
}());
