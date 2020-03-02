// Tests that query commands like find and aggregate accept a 'let' parameter which defines
// variables for use in expressions within the command.
(function() {
"use strict";

const coll = db.command_let_variables;
coll.drop();

assert.commandWorked(coll.insert([
    {
        Species: "Blackbird (Turdus merula)",
        population_trends: [
            {term: {start: 1970, end: 2014}, pct_change: -16, annual: -0.38, trend: "no change"},
            {term: {start: 2009, end: 2014}, pct_change: -2, annual: -0.36, trend: "no change"}
        ]
    },
    {
        Species: "Bullfinch (Pyrrhula pyrrhula)",
        population_trends: [
            {term: {start: 1970, end: 2014}, pct_change: -39, annual: -1.13, trend: "no change"},
            {term: {start: 2009, end: 2014}, pct_change: 12, annual: 2.38, trend: "weak increase"}
        ]
    },
    {
        Species: "Chaffinch (Fringilla coelebs)",
        population_trends: [
            {term: {start: 1970, end: 2014}, pct_change: 27, annual: 0.55, trend: "no change"},
            {term: {start: 2009, end: 2014}, pct_change: -7, annual: -1.49, trend: "weak decline"}
        ]
    },
    {
        Species: "Song Thrush (Turdus philomelos)",
        population_trends: [
            {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"},
            {term: {start: 2009, end: 2014}, pct_change: -4, annual: -0.88, trend: "no change"}
        ]
    }
]));

const pipeline = [
    {$project: {_id: 0}},
    {$unwind: "$population_trends"},
    {$match: {$expr: {$eq: ["$population_trends.trend", "$$target_trend"]}}},
    {$sort: {Species: 1}}
];
let expectedResults = [{
    Species: "Bullfinch (Pyrrhula pyrrhula)",
    population_trends:
        {term: {start: 2009, end: 2014}, pct_change: 12, annual: 2.38, trend: "weak increase"}
}];
assert.eq(coll.aggregate(pipeline, {let : {target_trend: "weak increase"}}).toArray(),
          expectedResults);
expectedResults = [
    {
        Species: "Chaffinch (Fringilla coelebs)",
        population_trends:
            {term: {start: 2009, end: 2014}, pct_change: -7, annual: -1.49, trend: "weak decline"}
    },
    {
        Species: "Song Thrush (Turdus philomelos)",
        population_trends:
            {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"}
    }
];
assert.eq(coll.aggregate(pipeline, {let : {target_trend: "weak decline"}}).toArray(),
          expectedResults);

// Negative test, not sure how this is supposed to fail. Variables only accessible in $expr.
db.runCommand({
    find: coll.getName(),
    let : {target_species: "weak decline"},
    filter: {Species: "$$target_species"}
});

// find
let result = assert
                 .commandWorked(db.runCommand({
                     find: coll.getName(),
                     let : {target_species: "Song Thrush (Turdus philomelos)"},
                     filter: {$expr: {$eq: ["$Species", "$$target_species"]}},
                     projection: {_id: 0}
                 }))
                 .cursor.firstBatch;
expectedResults = {
    Species: "Song Thrush (Turdus philomelos)",
    population_trends: [
        {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"},
        {term: {start: 2009, end: 2014}, pct_change: -4, annual: -0.88, trend: "no change"}
    ]
};

assert.eq(result.length, 1);
assert.eq(expectedResults, result[0]);

// findAndModify
assert.commandWorked(coll.insert({Species: "spy_bird"}));
result = db.runCommand({
    findAndModify: coll.getName(),
    let : {target_species: "spy_bird"},
    query: {$expr: {$eq: ["$Species", "$$target_species"]}},
    update: {Species: "questionable_bird"},
    fields: {_id: 0},
    new: true
});
assert.eq(result.value, {Species: "questionable_bird"}, result);

result = db.runCommand({
    findAndModify: coll.getName(),
    let : {species_name: "not_a_bird", realSpecies: "dino"},
    query: {$expr: {$eq: ["$Species", "questionable_bird"]}},
    update: [{$project: {Species: "$$species_name"}}, {$addFields: {suspect: "$$realSpecies"}}],
    fields: {_id: 0},
    new: true
});
assert.eq(result.value, {Species: "not_a_bird", suspect: "dino"}, result);
// delete
// result = assert.commandWorked(db.runCommand({
// delete: coll.getName(),
// let : {target_species: "not_a_bird"},
// deletes: [{
// q: {$expr: {$eq: ["$Species", "$$target_species"]}},
// limit: 0
// }]
// }));

// update
assert.commandWorked(db.runCommand({
    update: coll.getName(),
    let : {target_species: "Song Thrush (Turdus philomelos)", new_name: "Song Thrush"},
    updates: [
        {q: {$expr: {$eq: ["$Species", "$$target_species"]}}, u: [{$set: {Species: "$$new_name"}}]}
    ]
}));

// Test when the same variable is declared in the aggregate scope and $lookup scope.
// Create a second collection to lookup into
const lookup_coll = db.command_let_variables_lookup;
assert.commandWorked(lookup_coll.insert({Species: "Song Thrush", color: "brown"}));
assert.commandWorked(lookup_coll.insert({Species: "Chaffinch (Fringilla coelebs)", color: "red"}));
result = assert.commandWorked(db.runCommand({
    aggregate: coll.getName(),
    let: {target_species: "Song Thrush", overlappedVal: true},
    pipeline: [
        {$lookup: {
            from: lookup_coll.getName(),
            let: {funFact: "string", overlappedVal: false},
            pipeline: [
                // Test access to aggregation variables
                {$match: {$expr: {$eq: ["$Species", "$$target_species"]}}},
                // Test access to pipeline variables, and overwritten variables
                {$project: {Species: 1, color: 1, funfact: "$$funFact", val: "$$overlappedVal"}},
            ],
            as: "facts"
        }},
        {$match: {$expr: {$eq: ["$Species", "$$target_species"]}}},
    ],
    cursor: {}
}));

let lookup_results = result.cursor.firstBatch;
print(tojson(lookup_results));
// Make sure the match was applied to the correct species.
assert.eq(lookup_results.length, 1, lookup_results);
assert.eq(lookup_results[0].Species, "Song Thrush", lookup_results[0]);
assert.eq(lookup_results[0].facts[0].val, false, lookup_results[0]);

result = assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    let: {target_species: "Song Thrush", overlappedVal: true},
    pipeline: [
        {$lookup: {
            from: lookup_coll.getName(),
            let: {funFact: "string", overlappedVal: false},
            pipeline: [
                // Test access to aggregation variables
                {$match: {$expr: {$eq: ["$Species", "$$target_species"]}}},
                // Test access to pipeline variables, and overwritten variables
                {$project: {Species: 1, color: 1, funfact: "$$funFact", val: "$$overlappedVal"}},
            ],
            as: "facts"
        }},
        {$match: {$expr: {$eq: ["$Species", "$$target_species"]}}},
        {$project: {bonusFact: "$$funFact"}}
    ],
    cursor: {}
}), 17276);

// Test that runTimeConstants and let are equivalent using find.
result = assert
             .commandWorked(db.runCommand({
                 find: coll.getName(),
                 runtimeConstants: {target_species: "Song Thrush"},
                 filter: {$expr: {$eq: ["$Species", "$$target_species"]}},
                 projection: {_id: 0}
             }))
             .cursor.firstBatch;
expectedResults = {
    Species: "Song Thrush",
    population_trends: [
        {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"},
        {term: {start: 2009, end: 2014}, pct_change: -4, annual: -0.88, trend: "no change"}
    ]
};

assert.eq(result.length, 1);
assert.eq(expectedResults, result[0]);

// Test that if runtimeConstants and let are both specified, the later one wins.
result = assert
             .commandWorked(db.runCommand({
                 find: coll.getName(),
                 let : {target_species: "Song Thrush"},
                 runtimeConstants: {target_species: "not_a_bird"},
                 filter: {$expr: {$eq: ["$Species", "$$target_species"]}},
                 projection: {_id: 0}
             }))
             .cursor.firstBatch;
expectedResults = {
    Species: "Song Thrush",
    population_trends: [
        {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"},
        {term: {start: 2009, end: 2014}, pct_change: -4, annual: -0.88, trend: "no change"}
    ]
};

assert.eq(result.length, 1);
assert.eq(expectedResults, result[0]);
result = assert
             .commandWorked(db.runCommand({
                 find: coll.getName(),
                 runtimeConstants: {target_species: "Song Thrush"},
                 let : {target_species: "not_a_bird"},
                 filter: {$expr: {$eq: ["$Species", "$$target_species"]}},
                 projection: {_id: 0}
             }))
             .cursor.firstBatch;
expectedResults = {
    Species: "Song Thrush",
    population_trends: [
        {term: {start: 1970, end: 2014}, pct_change: -53, annual: -1.7, trend: "weak decline"},
        {term: {start: 2009, end: 2014}, pct_change: -4, annual: -0.88, trend: "no change"}
    ]
};

assert.eq(result.length, 1);
assert.eq(expectedResults, result[0]);
}());
