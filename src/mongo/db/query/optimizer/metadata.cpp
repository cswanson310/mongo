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

#include "mongo/db/query/optimizer/metadata.h"
#include "mongo/base/init.h"
#include "mongo/db/query/optimizer/abt_hash.h"
#include "mongo/db/query/optimizer/node.h"

namespace mongo::optimizer {

IntervalRequirementDNF kIntervalReqFullyOpenDNF;
MONGO_INITIALIZER(kIntervalReqFullyOpenDNF)(InitializerContext* context) {
    kIntervalReqFullyOpenDNF.emplace_back(std::vector<IntervalRequirement>{IntervalRequirement{}});
}

DistributionAndPaths::DistributionAndPaths(DistributionType type)
    : DistributionAndPaths(type, {}) {}

DistributionAndPaths::DistributionAndPaths(DistributionType type, ABTVector paths)
    : _type(type), _paths(std::move(paths)) {}

BoundRequirement::BoundRequirement() : _inclusive(false), _bound(make<Blackhole>()) {}

BoundRequirement::BoundRequirement(bool inclusive, ABT bound)
    : _inclusive(inclusive), _bound(std::move(bound)) {
    uassert(0, "Infinite bound cannot be inclusive", !inclusive || !isInfinite());
}

bool BoundRequirement::operator==(const BoundRequirement& other) const {
    return _inclusive == other._inclusive && _bound == other._bound;
}

bool BoundRequirement::isInclusive() const {
    return _inclusive;
}

bool BoundRequirement::isInfinite() const {
    return _bound == make<Blackhole>();
}

const ABT& BoundRequirement::getBound() const {
    uassert(0, "Cannot retrieve infinite bound", !isInfinite());
    return _bound;
}

IntervalRequirement::IntervalRequirement(BoundRequirement lowBound, BoundRequirement highBound)
    : _lowBound(std::move(lowBound)), _highBound(std::move(highBound)) {}

bool IntervalRequirement::operator==(const IntervalRequirement& other) const {
    return _lowBound == other._lowBound && _highBound == other._highBound;
}

bool IntervalRequirement::isFullyOpen() const {
    return _lowBound.isInfinite() && _highBound.isInfinite();
}

bool IntervalRequirement::isEquality() const {
    return _lowBound.isInclusive() && _highBound.isInclusive() && _lowBound == _highBound;
}

const BoundRequirement& IntervalRequirement::getLowBound() const {
    return _lowBound;
}

BoundRequirement& IntervalRequirement::getLowBound() {
    return _lowBound;
}

const BoundRequirement& IntervalRequirement::getHighBound() const {
    return _highBound;
}

BoundRequirement& IntervalRequirement::getHighBound() {
    return _highBound;
}

PartialSchemaKey::PartialSchemaKey() : PartialSchemaKey({}, make<PathIdentity>()) {}

PartialSchemaKey::PartialSchemaKey(ProjectionName projectionName)
    : PartialSchemaKey(std::move(projectionName), make<PathIdentity>()) {}

PartialSchemaKey::PartialSchemaKey(ProjectionName projectionName, ABT path)
    : _projectionName(std::move(projectionName)), _path(std::move(path)) {
    assertPathSort(_path);
}

bool PartialSchemaKey::operator==(const PartialSchemaKey& other) const {
    return _projectionName == other._projectionName && _path == other._path;
}

bool PartialSchemaKey::emptyPath() const {
    return _path == make<PathIdentity>();
}

FieldNameType PartialSchemaKey::getSimpleField() const {
    const PathGet* pathGet = _path.cast<PathGet>();
    return (pathGet != nullptr && pathGet->getPath() == make<PathIdentity>()) ? pathGet->name()
                                                                              : "";
}

PartialSchemaRequirement::PartialSchemaRequirement() : _intervals(kIntervalReqFullyOpenDNF) {}

PartialSchemaRequirement::PartialSchemaRequirement(ProjectionName boundProjectionName,
                                                   IntervalRequirementDNF intervals)
    : _boundProjectionName(std::move(boundProjectionName)), _intervals(std::move(intervals)) {}

bool PartialSchemaRequirement::operator==(const PartialSchemaRequirement& other) const {
    return _boundProjectionName == other._boundProjectionName && _intervals == other._intervals;
}

bool PartialSchemaRequirement::hasBoundProjectionName() const {
    return !_boundProjectionName.empty();
}

const ProjectionName& PartialSchemaRequirement::getBoundProjectionName() const {
    return _boundProjectionName;
}

void PartialSchemaRequirement::setBoundProjectionName(ProjectionName boundProjectionName) {
    _boundProjectionName = std::move(boundProjectionName);
}

const IntervalRequirementDNF& PartialSchemaRequirement::getIntervals() const {
    return _intervals;
}

IntervalRequirementDNF& PartialSchemaRequirement::getIntervals() {
    return _intervals;
}

size_t PartialSchemaKeyHash::operator()(const PartialSchemaKey& key) const {
    return 31 * std::hash<std::string>()(key._projectionName) +
        ABTHashGenerator::generateIncludeChildren(key._path);
}

bool CandidateIndexEntry::operator==(const CandidateIndexEntry& other) const {
    return _fieldProjectionMap == other._fieldProjectionMap && _intervals == other._intervals &&
        _fieldsToCollate == other._fieldsToCollate;
}

IndexSpecification::IndexSpecification(std::string scanDefName,
                                       std::string indexDefName,
                                       MultiKeyIntervalRequirementDNF intervals,
                                       bool reverseOrder)
    : _scanDefName(std::move(scanDefName)),
      _indexDefName(std::move(indexDefName)),
      _intervals(std::move(intervals)),
      _reverseOrder(reverseOrder) {}

bool IndexSpecification::operator==(const IndexSpecification& other) const {
    return _scanDefName == other._scanDefName && _indexDefName == other._indexDefName &&
        _intervals == other._intervals && _reverseOrder == other._reverseOrder;
}

const std::string& IndexSpecification::getScanDefName() const {
    return _scanDefName;
}

const std::string& IndexSpecification::getIndexDefName() const {
    return _indexDefName;
}

const MultiKeyIntervalRequirementDNF& IndexSpecification::getIntervals() const {
    return _intervals;
}

bool IndexSpecification::isReverseOrder() const {
    return _reverseOrder;
}

static ABT buildABTFromFieldPath(FieldPathType fieldPath) {
    ABT result = make<PathIdentity>();
    for (size_t i = fieldPath.size(); i-- > 0;) {
        result = make<PathGet>(fieldPath.at(i), make<PathTraverse>(std::move(result)));
    }
    return result;
}

IndexCollationEntry::IndexCollationEntry(FieldPathType fieldPath, CollationOp op)
    : IndexCollationEntry(buildABTFromFieldPath(std::move(fieldPath)), op) {}

bool IndexCollationEntry::operator==(const IndexCollationEntry& other) const {
    return _path == other._path && _op == other._op;
}

IndexCollationEntry::IndexCollationEntry(ABT path, CollationOp op)
    : _path(std::move(path)), _op(op) {}

IndexDefinition::IndexDefinition(IndexCollationSpec collationSpec)
    : IndexDefinition(std::move(collationSpec),
                      2 /*version*/,
                      0 /*orderingBits*/,
                      {DistributionType::Centralized},
                      {}) {}

IndexDefinition::IndexDefinition(IndexCollationSpec collationSpec,
                                 int64_t version,
                                 uint32_t orderingBits,
                                 DistributionAndPaths distributionAndPaths,
                                 PartialSchemaRequirements partialReqMap)
    : _collationSpec(std::move(collationSpec)),
      _version(version),
      _orderingBits(orderingBits),
      _distributionAndPaths(distributionAndPaths),
      _partialReqMap(std::move(partialReqMap)) {}

const IndexCollationSpec& IndexDefinition::getCollationSpec() const {
    return _collationSpec;
}

int64_t IndexDefinition::getVersion() const {
    return _version;
}

uint32_t IndexDefinition::getOrdering() const {
    return _orderingBits;
}

const DistributionAndPaths& IndexDefinition::getDistributionAndPaths() const {
    return _distributionAndPaths;
}

const PartialSchemaRequirements& IndexDefinition::getPartialReqMap() const {
    return _partialReqMap;
}

ScanDefinition::ScanDefinition() : ScanDefinition(OptionsMapType{}, {}) {}

ScanDefinition::ScanDefinition(OptionsMapType options,
                               std::unordered_map<std::string, IndexDefinition> indexDefs)
    : ScanDefinition(std::move(options), std::move(indexDefs), {DistributionType::Centralized}) {}

ScanDefinition::ScanDefinition(OptionsMapType options,
                               std::unordered_map<std::string, IndexDefinition> indexDefs,
                               DistributionAndPaths distributionAndPaths)
    : _options(std::move(options)),
      _distributionAndPaths(std::move(distributionAndPaths)),
      _indexDefs(std::move(indexDefs)) {}

const ScanDefinition::OptionsMapType& ScanDefinition::getOptionsMap() const {
    return _options;
}

const DistributionAndPaths& ScanDefinition::getDistributionAndPaths() const {
    return _distributionAndPaths;
}

const std::unordered_map<std::string, IndexDefinition>& ScanDefinition::getIndexDefs() const {
    return _indexDefs;
}

std::unordered_map<std::string, IndexDefinition>& ScanDefinition::getIndexDefs() {
    return _indexDefs;
}

Metadata::Metadata(std::unordered_map<std::string, ScanDefinition> scanDefs)
    : Metadata(std::move(scanDefs), 1 /*numberOfPartitions*/) {}

Metadata::Metadata(std::unordered_map<std::string, ScanDefinition> scanDefs,
                   size_t numberOfPartitions)
    : _scanDefs(std::move(scanDefs)), _numberOfPartitions(numberOfPartitions) {}

}  // namespace mongo::optimizer
