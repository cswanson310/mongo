/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <map>

#include "mongo/db/query/optimizer/defs.h"
#include "mongo/db/query/optimizer/syntax/syntax.h"

namespace mongo::optimizer {

struct DistributionAndPaths {
    DistributionAndPaths(DistributionType type);
    DistributionAndPaths(DistributionType type, ABTVector paths);

    DistributionType _type;
    ABTVector _paths;
};

class BoundRequirement {
public:
    BoundRequirement();
    BoundRequirement(bool inclusive, ABT bound);

    bool operator==(const BoundRequirement& other) const;

    bool isInclusive() const;
    bool isInfinite() const;
    const ABT& getBound() const;

private:
    bool _inclusive;
    ABT _bound;
};

class IntervalRequirement {
public:
    IntervalRequirement() = default;
    IntervalRequirement(BoundRequirement lowBound, BoundRequirement highBound);

    bool operator==(const IntervalRequirement& other) const;

    bool isFullyOpen() const;
    bool isEquality() const;

    const BoundRequirement& getLowBound() const;
    BoundRequirement& getLowBound();
    const BoundRequirement& getHighBound() const;
    BoundRequirement& getHighBound();

private:
    BoundRequirement _lowBound;
    BoundRequirement _highBound;
};

struct PartialSchemaKey {
    PartialSchemaKey();
    PartialSchemaKey(ProjectionName projectionName);
    PartialSchemaKey(ProjectionName projectionName, ABT path);

    bool operator==(const PartialSchemaKey& other) const;

    bool emptyPath() const;
    FieldNameType getSimpleField() const;

    // Referred, or input projection name.
    ProjectionName _projectionName;

    // (Partially determined) path.
    ABT _path;
};

// Disjunction of conjunctions of intervals (DNF).
using IntervalRequirementDNF = std::vector<std::vector<IntervalRequirement>>;
extern IntervalRequirementDNF kIntervalReqFullyOpenDNF;

class PartialSchemaRequirement {
public:
    PartialSchemaRequirement();
    PartialSchemaRequirement(ProjectionName boundProjectionName, IntervalRequirementDNF intervals);

    bool operator==(const PartialSchemaRequirement& other) const;

    bool hasBoundProjectionName() const;
    const ProjectionName& getBoundProjectionName() const;
    void setBoundProjectionName(ProjectionName boundProjectionName);

    const IntervalRequirementDNF& getIntervals() const;
    IntervalRequirementDNF& getIntervals();

private:
    // Bound, or output projection name.
    ProjectionName _boundProjectionName;

    IntervalRequirementDNF _intervals;
};

struct PartialSchemaKeyHash {
    size_t operator()(const PartialSchemaKey& key) const;
};

// Map from referred (or input) projection name to list of requirements for that projection.
using PartialSchemaRequirements =
    std::unordered_map<PartialSchemaKey, PartialSchemaRequirement, PartialSchemaKeyHash>;

// A sequence of intervals corresponding, one for each index key.
using MultiKeyIntervalRequirement = std::vector<IntervalRequirement>;

// Multi-key intervals represent unions and conjunctions of individual multi-key intervals in DNF
// form: Example: (interval1 ^ interval2) union (interval3 ^ interval4).
using MultiKeyIntervalRequirementDNF = std::vector<std::vector<MultiKeyIntervalRequirement>>;

// Used to pre-compute candidate indexes for SargableNodes.
struct CandidateIndexEntry {
    bool operator==(const CandidateIndexEntry& other) const;

    FieldProjectionMap _fieldProjectionMap;
    MultiKeyIntervalRequirementDNF _intervals;

    // We have equalities on those index fields, and thus do not consider for collation
    // requirements.
    // TODO: consider a bitset.
    std::unordered_set<size_t> _fieldsToCollate;
};

using CandidateIndexMap = std::unordered_map<std::string, CandidateIndexEntry>;

class IndexSpecification {
public:
    IndexSpecification(std::string scanDefName,
                       std::string indexDefName,
                       MultiKeyIntervalRequirementDNF intervals,
                       bool reverseOrder);

    bool operator==(const IndexSpecification& other) const;

    const std::string& getScanDefName() const;
    const std::string& getIndexDefName() const;

    const MultiKeyIntervalRequirementDNF& getIntervals() const;
    MultiKeyIntervalRequirementDNF& getIntervals();

    bool isReverseOrder() const;

private:
    // Name of the collection.
    const std::string _scanDefName;

    // The name of the index.
    const std::string _indexDefName;

    // The index intervals in DNF form.
    MultiKeyIntervalRequirementDNF _intervals;

    // Do we reverse the index order.
    const bool _reverseOrder;
};

struct IndexCollationEntry {
    IndexCollationEntry(FieldPathType fieldPath, CollationOp op);
    IndexCollationEntry(ABT path, CollationOp op);

    bool operator==(const IndexCollationEntry& other) const;

    ABT _path;
    CollationOp _op;
};

using IndexCollationSpec = std::vector<IndexCollationEntry>;

/**
 * Defines an available system index.
 */
class IndexDefinition {
public:
    // For testing.
    IndexDefinition(IndexCollationSpec collationSpec);

    IndexDefinition(IndexCollationSpec collationSpec,
                    int64_t version,
                    uint32_t orderingBits,
                    DistributionAndPaths distributionAndPaths,
                    PartialSchemaRequirements partialReqMap);

    const IndexCollationSpec& getCollationSpec() const;

    int64_t getVersion() const;
    uint32_t getOrdering() const;

    const DistributionAndPaths& getDistributionAndPaths() const;

    const PartialSchemaRequirements& getPartialReqMap() const;

private:
    const IndexCollationSpec _collationSpec;

    const int64_t _version;
    const uint32_t _orderingBits;

    DistributionAndPaths _distributionAndPaths;

    // Requirements map for partial filter expression.
    const PartialSchemaRequirements _partialReqMap;
};

// Used to specify parameters to scan node, such as collection name, or file where collection is
// read from.
class ScanDefinition {
public:
    using OptionsMapType = std::map<std::string, std::string>;

    ScanDefinition();
    ScanDefinition(OptionsMapType options, std::map<std::string, IndexDefinition> indexDefs);
    ScanDefinition(OptionsMapType options,
                   std::map<std::string, IndexDefinition> indexDefs,
                   DistributionAndPaths distributionAndPaths);

    const OptionsMapType& getOptionsMap() const;

    const DistributionAndPaths& getDistributionAndPaths() const;

    const std::map<std::string, IndexDefinition>& getIndexDefs() const;
    std::map<std::string, IndexDefinition>& getIndexDefs();

private:
    OptionsMapType _options;
    DistributionAndPaths _distributionAndPaths;

    /**
     * Indexes associated with this collection.
     */
    std::map<std::string, IndexDefinition> _indexDefs;
};

struct Metadata {
    Metadata(std::map<std::string, ScanDefinition> scanDefs);
    Metadata(std::map<std::string, ScanDefinition> scanDefs, size_t numberOfPartitions);

    std::map<std::string, ScanDefinition> _scanDefs;

    // TODO: generalize cluster spec and hints.

    // Local parallelism.
    size_t _numberOfPartitions;
};

}  // namespace mongo::optimizer
