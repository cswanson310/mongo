// Tests that a $unionWith stage can read from a view.
(function() {
"use strict";

const coll = db.read_from_view;
coll.drop();
const viewName = "view_to_read";
const view = db[viewName];
view.drop();
assert.commandWorked(db.runCommand({create: viewName, viewOn: coll.getName(), pipeline: []}));

assert.commandWorked(coll.insert([{_id: 0}, {_id: 1}]));
assert.eq(2, coll.aggregate([]).itcount());
assert.eq(4, coll.aggregate([{$unionWith: viewName}]).itcount());
assert.eq(
    3, coll.aggregate([{$unionWith: {coll: viewName, pipeline: [{$match: {_id: 1}}]}}]).itcount());

// Now test with a non-empty view.
view.drop();
assert.commandWorked(
    db.runCommand({create: viewName, viewOn: coll.getName(), pipeline: [{$match: {_id: 0}}]}));
assert.eq(2, coll.aggregate([]).itcount());
assert.eq(3, coll.aggregate([{$unionWith: viewName}]).itcount());
assert.eq(
    2, coll.aggregate([{$unionWith: {coll: viewName, pipeline: [{$match: {_id: 1}}]}}]).itcount());
}());
