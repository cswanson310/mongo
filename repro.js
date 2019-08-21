// Designed to reproduce duplicate key errors from the BF I'm working on.
(function() {
"use strict";

const st = new ShardingTest({shards: 2});

const source = st.getDB("test").source;
const target = st.getDB("test").target;
st.shardColl(source, {_id: 1}, {_id: 10}, {_id: 0});
st.shardColl(target, {_id: 1}, {_id: 10}, {_id: 0});

assert.commandWorked(source.insert(Array.from({length: 20}, (_, i) => ({_id: i}))));

for (let trial = 0; trial < 2000; trial++) {
    assert.commandWorked(target.insert([{_id: 4}]));  // Force a duplicate key.
    const pipeline = [
            {
              $project: {
                  _id: {
                      $let: {
                          vars: {shifted: {$subtract: ["$_id", 5]}},
                          in : {
                              $cond: {
                                  if: {$lt: ["$$shifted", 0]},
                                  then: {$add: ["$$shifted", 20]},
                                  else: "$$shifted"
                              }
                          }
                      }
                  }
              }
            },
            {$merge: {into: target.getName(), whenMatched: "fail"}}
        ];
    if (trial == 0) {
        jsTestLog(`Explain plan: ${tojson(source.explain().aggregate(pipeline))}`);
        jsTestLog(`Results: ${tojson(source.aggregate(pipeline.slice(0, 1)))}`);
    }
    const error = assert.throws(() => source.aggregate(pipeline));
    assert.eq(error.code, ErrorCodes.DuplicateKey);
    assert.commandWorked(target.remove({}));
    source.aggregate(pipeline);
    assert.commandWorked(target.remove({}));
    for (let attempt = 0; attempt < 5; attempt++) {
        assert.eq(target.find({}).itcount(), 0, target.find({}).toArray());
        sleep(4);
    }
}

st.stop();
})();

function unused() {
    "use strict";

    const st = new ShardingTest({shards: 2, config: 1});

    const mongosDB = st.s.getDB("test");
    const source = mongosDB.source;
    const target = mongosDB.target;

    // Shard collection, move some to each shard.
    st.shardColl(source, {_id: 1}, {_id: 1}, {_id: 2});

    assert.commandWorked(source.insert([{_id: 0}, {_id: 1}, {_id: 2}, {_id: 3}]));
    for (let i = 0; i < 500; i++) {
        assert.eq(0, target.find().itcount());
        assert.commandWorked(
            mongosDB.runCommand({create: target.getName(), validator: {_id: {$nin: [0, 2]}}}));
        st.shardColl(target, {_id: 1}, {_id: 1}, {_id: 2});
        const explain = source.explain().aggregate([{$merge: {into: target.getName()}}]);
        assert.throws(() => source.aggregate([{$merge: {into: target.getName()}}]));
        target.drop();
    }

    st.stop();
}
