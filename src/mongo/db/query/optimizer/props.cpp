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

#include "mongo/db/query/optimizer/props.h"
#include "mongo/db/query/optimizer/utils.h"
#include "mongo/util/assert_util.h"

namespace mongo::optimizer::properties {

CollationRequirement::CollationRequirement(ProjectionCollationSpec spec) : _spec(std::move(spec)) {
    ProjectionNameSet projections;
    for (const auto& entry : _spec) {
        uassert(6624000, "Repeated projection name", projections.insert(entry.first).second);
    }
}

CollationRequirement CollationRequirement::Empty = CollationRequirement();

bool CollationRequirement::operator==(const CollationRequirement& other) const {
    return _spec == other._spec;
}

bool CollationRequirement::isCompatibleWith(const CollationRequirement& other) const {
    return collationsCompatible(_spec, other._spec);
}

bool CollationRequirement::mergeWith(const CollationRequirement& other) {
    return isCompatibleWith(other);
}

const ProjectionCollationSpec& CollationRequirement::getCollationSpec() const {
    return _spec;
}

ProjectionNameSet CollationRequirement::getAffectedProjectionNames() const {
    ProjectionNameSet result;
    for (const auto& entry : _spec) {
        result.insert(entry.first);
    }
    return result;
}

LimitSkipRequirement::LimitSkipRequirement(const IntType limit, const IntType skip)
    : LimitSkipRequirement(limit, skip, false /*isEnforced*/) {}

LimitSkipRequirement::LimitSkipRequirement(const IntType limit, const IntType skip, bool isEnforced)
    : _limit((limit < 0) ? kMaxVal : limit), _skip(skip), _isEnforced(isEnforced) {}

bool LimitSkipRequirement::operator==(const LimitSkipRequirement& other) const {
    return _skip == other._skip && _limit == other._limit && _isEnforced == other._isEnforced;
}

bool LimitSkipRequirement::isCompatibleWith(const LimitSkipRequirement& other) const {
    return _skip >= other._skip && getAbsoluteLimit() <= other.getAbsoluteLimit();
}

bool LimitSkipRequirement::mergeWith(const LimitSkipRequirement& other) {
    uassert(0, "Cannot merge enforced requirements", !_isEnforced && !other._isEnforced);

    // TODO: is this correct?
    const IntType newAbsLimit = std::min<IntType>(
        hasNoLimit() ? kMaxVal : (other._skip + getAbsoluteLimit()),
        std::max<IntType>(0, other.hasNoLimit() ? kMaxVal : (other.getAbsoluteLimit() - _skip)));

    _limit = (newAbsLimit == kMaxVal) ? kMaxVal : (newAbsLimit - other._skip);
    _skip = (_limit == 0) ? 0 : other._skip;

    return true;
}

LimitSkipRequirement::IntType LimitSkipRequirement::getLimit() const {
    return _limit;
}

LimitSkipRequirement::IntType LimitSkipRequirement::getSkip() const {
    return _skip;
}

LimitSkipRequirement::IntType LimitSkipRequirement::getAbsoluteLimit() const {
    return hasNoLimit() ? kMaxVal : (_skip + _limit);
}

ProjectionNameSet LimitSkipRequirement::getAffectedProjectionNames() const {
    return {};
}

bool LimitSkipRequirement::isEnforced() const {
    return _isEnforced;
}

bool LimitSkipRequirement::hasNoLimit() const {
    return _limit == kMaxVal;
}

ProjectionRequirement::ProjectionRequirement(ProjectionNameOrderPreservingSet projections)
    : _projections(std::move(projections)) {}

bool ProjectionRequirement::operator==(const ProjectionRequirement& other) const {
    // We are equal to other (potentially reordered) projections, but maintain our given order.
    return isCompatibleWith(other._projections);
}

bool ProjectionRequirement::isCompatibleWith(const ProjectionRequirement& other) const {
    // Do we have a projection superset (not necessarily strict superset)?
    for (const ProjectionName& projectionName : other.getProjections().getVector()) {
        if (!_projections.find(projectionName).second) {
            return false;
        }
    }
    return true;
}

bool ProjectionRequirement::mergeWith(const ProjectionRequirement& other) {
    return isCompatibleWith(other);
}

ProjectionNameSet ProjectionRequirement::getAffectedProjectionNames() const {
    ProjectionNameSet result;
    for (const ProjectionName& projection : _projections.getVector()) {
        result.insert(projection);
    }
    return result;
}

const ProjectionNameOrderPreservingSet& ProjectionRequirement::getProjections() const {
    return _projections;
}

ProjectionNameOrderPreservingSet& ProjectionRequirement::getProjections() {
    return _projections;
}

DistributionRequirement::DistributionRequirement(DistributionType type)
    : DistributionRequirement(type, {}) {}

DistributionRequirement::DistributionRequirement(DistributionType type,
                                                 ProjectionNameVector projectionNames)
    : _type(type), _projectionNames(std::move(projectionNames)) {
    uassert(0,
            "Must have projection names when distributed under hash or range partitioning",
            (type != DistributionType::HashPartitioning &&
             type != DistributionType::RangePartitioning) ||
                !_projectionNames.empty());
}

bool DistributionRequirement::operator==(const DistributionRequirement& other) const {
    return _type == other._type && _projectionNames == other._projectionNames;
}

bool DistributionRequirement::isCompatibleWith(const DistributionRequirement& other) const {
    return *this == other;
}

bool DistributionRequirement::mergeWith(const DistributionRequirement& other) {
    return isCompatibleWith(other);
}

const ProjectionNameVector& DistributionRequirement::getProjections() const {
    return _projectionNames;
}

ProjectionNameSet DistributionRequirement::getAffectedProjectionNames() const {
    ProjectionNameSet result;
    for (const ProjectionName& projectionName : _projectionNames) {
        result.insert(projectionName);
    }
    return result;
}

IndexingRequirement::IndexingRequirement() : IndexingRequirement(IndexReqTarget::Complete, "") {}

IndexingRequirement::IndexingRequirement(IndexReqTarget indexReqTarget,
                                         ProjectionName ridProjectionName)
    : _indexReqTarget(indexReqTarget), _ridProjectionName(std::move(ridProjectionName)) {}

bool IndexingRequirement::operator==(const IndexingRequirement& other) const {
    return _indexReqTarget == other._indexReqTarget &&
        _ridProjectionName == other._ridProjectionName;
}

ProjectionNameSet IndexingRequirement::getAffectedProjectionNames() const {
    return {};
}

IndexReqTarget IndexingRequirement::getIndexReqTarget() const {
    return _indexReqTarget;
}

const ProjectionName& IndexingRequirement::getRIDProjectionName() const {
    return _ridProjectionName;
}

bool IndexingRequirement::isCompatibleWith(const IndexingRequirement& other) const {
    // For now we assume we are compatible if equal.
    return _indexReqTarget == other._indexReqTarget &&
        _ridProjectionName == other._ridProjectionName;
}

bool IndexingRequirement::mergeWith(const IndexingRequirement& other) {
    uasserted(6624000, "Should not be attempting to merge for indexing requirements.");
}

DistributionType DistributionRequirement::getType() const {
    return _type;
}

ProjectionAvailability::ProjectionAvailability(ProjectionNameSet projections)
    : _projections(std::move(projections)) {}

bool ProjectionAvailability::operator==(const ProjectionAvailability& other) const {
    return _projections == other._projections;
}

const ProjectionNameSet& ProjectionAvailability::getProjections() const {
    return _projections;
}

bool ProjectionAvailability::canSatisfy(const ProjectionRequirement& projectionRequirement) const {
    for (const ProjectionName& projectionName :
         projectionRequirement.getProjections().getVector()) {
        if (_projections.find(projectionName) == _projections.cend()) {
            return false;
        }
    }
    return true;
}

CardinalityEstimate::CardinalityEstimate(const CEType estimate) : _estimate(estimate) {}

bool CardinalityEstimate::operator==(const CardinalityEstimate& other) const {
    return _estimate == other._estimate;
}

CEType CardinalityEstimate::getEstimate() const {
    return _estimate;
}

CEType& CardinalityEstimate::getEstimate() {
    return _estimate;
}

IndexingAvailability::IndexingAvailability(
    GroupIdType scanGroupId,
    ProjectionName scanProjection,
    std::vector<ProjectionNameOrderPreservingSet> sargableProjectionNames)
    : _scanGroupId(scanGroupId),
      _scanProjection(std::move(scanProjection)),
      _sargableProjectionNames(std::move(sargableProjectionNames)) {}

bool IndexingAvailability::operator==(const IndexingAvailability& other) const {
    return _scanGroupId == other._scanGroupId && _scanProjection == other._scanProjection &&
        _sargableProjectionNames == other._sargableProjectionNames;
}

GroupIdType IndexingAvailability::getScanGroupId() const {
    return _scanGroupId;
}

const ProjectionName& IndexingAvailability::getScanProjection() const {
    return _scanProjection;
}

const std::vector<ProjectionNameOrderPreservingSet>&
IndexingAvailability::getSargableProjectionNames() const {
    return _sargableProjectionNames;
}

std::vector<ProjectionNameOrderPreservingSet>& IndexingAvailability::getSargableProjectionNames() {
    return _sargableProjectionNames;
}

CollectionAvailability::CollectionAvailability(std::unordered_set<std::string> scanDefSet)
    : _scanDefSet(std::move(scanDefSet)) {}

bool CollectionAvailability::operator==(const CollectionAvailability& other) const {
    return _scanDefSet == other._scanDefSet;
}

const std::unordered_set<std::string>& CollectionAvailability::getScanDefSet() const {
    return _scanDefSet;
}

size_t DistributionHash::operator()(const DistributionRequirement& distribution) const {
    size_t result = 0;
    updateHash(result, std::hash<DistributionType>()(distribution.getType()));
    for (const ProjectionName& projectionName : distribution.getProjections()) {
        updateHash(result, std::hash<ProjectionName>()(projectionName));
    }
    return result;
}

DistributionAvailability::DistributionAvailability(DistributionSet distributionSet)
    : _distributionSet(std::move(distributionSet)) {}

bool DistributionAvailability::operator==(const DistributionAvailability& other) const {
    return _distributionSet == other._distributionSet;
}

const DistributionSet& DistributionAvailability::getDistributionSet() const {
    return _distributionSet;
}

DistributionSet& DistributionAvailability::getDistributionSet() {
    return _distributionSet;
}

}  // namespace mongo::optimizer::properties
