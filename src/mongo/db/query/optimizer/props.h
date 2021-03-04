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
#include <string>
#include <vector>

#include "mongo/db/query/optimizer/algebra/operator.h"
#include "mongo/db/query/optimizer/algebra/polyvalue.h"
#include "mongo/db/query/optimizer/defs.h"
#include "mongo/util/assert_util.h"

namespace mongo::optimizer::properties {

/**
 * Tag for logical property types.
 */
class LogicalProperty {};

/**
 * Tag for physical property types.
 */
class PhysicalProperty {};

/**
 * Logical properties.
 */
class CardinalityEstimate;
// TODO: statistics (e.g. histograms)

class ProjectionAvailability;
class IndexingAvailability;
class CollectionAvailability;
class DistributionAvailability;

/**
 * Physical properties.
 */
class CollationRequirement;
class LimitSkipRequirement;
class ProjectionRequirement;
class DistributionRequirement;
class IndexingRequirement;

using Property = algebra::PolyValue<CardinalityEstimate,
                                    ProjectionAvailability,
                                    IndexingAvailability,
                                    CollectionAvailability,
                                    DistributionAvailability,
                                    CollationRequirement,
                                    LimitSkipRequirement,
                                    ProjectionRequirement,
                                    DistributionRequirement,
                                    IndexingRequirement>;


template <typename T, typename... Args>
inline auto makeProperty(Args&&... args) {
    return Property::make<T>(std::forward<Args>(args)...);
}

using Properties = std::map<Property::key_type, Property>;

template <class P>
static constexpr Property::key_type getPropertyKey() {
    return Property::tagOf<P>();
}

template <class P>
bool hasProperty(const Properties& props) {
    return props.find(getPropertyKey<P>()) != props.cend();
}

template <class P>
P& getProperty(Properties& props) {
    if (!hasProperty<P>(props)) {
        uasserted(6624000, "Property type does not exist.");
    }
    return *props.at(getPropertyKey<P>()).template cast<P>();
}

template <class P>
const P& getPropertyConst(const Properties& props) {
    if (!hasProperty<P>(props)) {
        uasserted(6624000, "Property type does not exist.");
    }
    return *props.at(getPropertyKey<P>()).template cast<P>();
}

template <class P>
void removeProperty(Properties& props) {
    props.erase(getPropertyKey<P>());
}

template <class P>
bool setProperty(Properties& props, P property) {
    return props.insert({getPropertyKey<P>(), makeProperty<P>(std::move(property))}).second;
}

template <class P>
void setPropertyOverwrite(Properties& props, P property) {
    removeProperty<P>(props);
    setProperty<P>(props, std::move(property));
}

template <class P>
bool mergeProperty(Properties& props, const P& property) {
    if (!hasProperty<P>(props)) {
        setProperty(props, property);
        return true;
    }

    return getProperty<P>(props).mergeWith(property);
}

template <class P>
bool isPropertyCompatible(const Properties& props, const P& property) {
    return hasProperty<P>(props) && getPropertyConst<P>(props).isCompatibleWith(property);
}

template <typename... Args>
inline auto makeProperties(Args&&... args) {
    Properties props;
    (setProperty(props, args), ...);
    return props;
}

/**
 * A physical property which specifies how the collection (or intermediate result) is required to be
 * collated (sorted).
 */
class CollationRequirement final : public PhysicalProperty {
public:
    static CollationRequirement Empty;

    CollationRequirement() = default;
    CollationRequirement(ProjectionCollationSpec spec);

    bool operator==(const CollationRequirement& other) const;

    bool isCompatibleWith(const CollationRequirement& other) const;

    bool mergeWith(const CollationRequirement& other);

    const ProjectionCollationSpec& getCollationSpec() const;

    ProjectionNameSet getAffectedProjectionNames() const;

private:
    const ProjectionCollationSpec _spec;
};

/**
 * A physical property which specifies what portion of the result in terms of window defined by the
 * limit and skip is to be returned.
 */
class LimitSkipRequirement final : public PhysicalProperty {
public:
    using IntType = int64_t;
    static constexpr IntType kMaxVal = std::numeric_limits<IntType>::max();

    LimitSkipRequirement(IntType limit, IntType skip);
    LimitSkipRequirement(IntType limit, IntType skip, bool isEnforced);

    bool operator==(const LimitSkipRequirement& other) const;

    bool isCompatibleWith(const LimitSkipRequirement& other) const;

    bool mergeWith(const LimitSkipRequirement& other);

    bool hasNoLimit() const;

    IntType getLimit() const;
    IntType getSkip() const;
    IntType getAbsoluteLimit() const;

    ProjectionNameSet getAffectedProjectionNames() const;

    bool isEnforced() const;

private:
    // Max number of documents to return. Maximum integer value means unlimited.
    IntType _limit;
    // Documents to skip before start returning in result.
    IntType _skip;

    // Specifies if the requirement is already enforced (LimitSkipNode has been planced), and so we
    // are only considering the property as CE modifier.
    bool _isEnforced;
};

/**
 * A physical property which specifies required projections to be returned as part of the result.
 */
class ProjectionRequirement final : public PhysicalProperty {
public:
    ProjectionRequirement(ProjectionNameOrderPreservingSet projections);

    bool operator==(const ProjectionRequirement& other) const;

    bool isCompatibleWith(const ProjectionRequirement& other) const;

    bool mergeWith(const ProjectionRequirement& other);

    const ProjectionNameOrderPreservingSet& getProjections() const;
    ProjectionNameOrderPreservingSet& getProjections();

    ProjectionNameSet getAffectedProjectionNames() const;

private:
    ProjectionNameOrderPreservingSet _projections;
};

/**
 * A physical property which specifies how the result is to be distributed (or partitioned) amongst
 * the computing partitions/nodes.
 */
class DistributionRequirement final : public PhysicalProperty {
public:
    DistributionRequirement(DistributionType type);

    DistributionRequirement(DistributionType type, ProjectionNameVector projectionNames);

    bool operator==(const DistributionRequirement& other) const;

    bool isCompatibleWith(const DistributionRequirement& other) const;

    bool mergeWith(const DistributionRequirement& other);

    DistributionType getType() const;

    const ProjectionNameVector& getProjections() const;

    ProjectionNameSet getAffectedProjectionNames() const;

private:
    const DistributionType _type;

    /**
     * Defined for hash and range-based partitioning.
     */
    const ProjectionNameVector _projectionNames;
};

#define INDEXREQTARGET_NAMES(F) \
    F(Index)                    \
    F(Seek)                     \
    F(Complete)

MAKE_PRINTABLE_ENUM(IndexReqTarget, INDEXREQTARGET_NAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(IndexReqTargetEnum, IndexReqTarget, INDEXREQTARGET_NAMES);
#undef INDEXREQTARGET_NAMES

/**
 * A physical property which describes if we intend to satisfy sargable predicates using an index.
 * With indexing requirement "Complete", we are requiring a regular physical
 * scan (both rid and row). With "Seek" (where we must have a non-empty RID projection name), we are
 * targeting a physical Seek. With "Index" (with or without RID projection name), we
 * are targeting a physical IndexScan. If in this case we have set RID projection, then we have
 * either gone for a Seek, or we have performed intersection. With empty RID we are targeting a
 * covered index scan.
 */
class IndexingRequirement final : public PhysicalProperty {
public:
    IndexingRequirement();
    IndexingRequirement(IndexReqTarget indexReqTarget, ProjectionName ridProjectionName);

    bool operator==(const IndexingRequirement& other) const;

    ProjectionNameSet getAffectedProjectionNames() const;

    bool isCompatibleWith(const IndexingRequirement& other) const;

    bool mergeWith(const IndexingRequirement& other);

    IndexReqTarget getIndexReqTarget() const;

    const ProjectionName& getRIDProjectionName() const;

private:
    const IndexReqTarget _indexReqTarget;
    ProjectionName _ridProjectionName;
};

/**
 * A logical property which specifies available projections for a given ABT tree.
 */
class ProjectionAvailability final : public LogicalProperty {
public:
    ProjectionAvailability(ProjectionNameSet projections);

    bool operator==(const ProjectionAvailability& other) const;

    bool canSatisfy(const ProjectionRequirement& projectionRequirement) const;

    const ProjectionNameSet& getProjections() const;

private:
    ProjectionNameSet _projections;
};

/**
 * A logical property which provides an estimated row count for a given ABT tree.
 */
class CardinalityEstimate final : public LogicalProperty {
public:
    CardinalityEstimate(const CEType estimate);

    bool operator==(const CardinalityEstimate& other) const;

    CEType getEstimate() const;
    CEType& getEstimate();

private:
    CEType _estimate;
};

/**
 * A logical property which specifies availability to index predicates in the ABT subtree and
 * contains the scan projection. The projection and definition name are here for convenience: it can
 * be retrieved using the scan group from the memo.
 * In addition, we keep track of sargable projection names (extracted from EvalNodes) for use in
 * generating available distributions (for partitioned collection and indexes).
 */
class IndexingAvailability final : public LogicalProperty {
public:
    IndexingAvailability(GroupIdType scanGroupId,
                         ProjectionName scanProjection,
                         std::vector<ProjectionNameOrderPreservingSet> sargableProjectionNames);

    bool operator==(const IndexingAvailability& other) const;

    GroupIdType getScanGroupId() const;
    const ProjectionName& getScanProjection() const;

    const std::vector<ProjectionNameOrderPreservingSet>& getSargableProjectionNames() const;
    std::vector<ProjectionNameOrderPreservingSet>& getSargableProjectionNames();

private:
    const GroupIdType _scanGroupId;
    const ProjectionName _scanProjection;

    // TODO: this is a superset of the viable projection names. In the fullness of time we need to
    // keep track of only the projection names extracted via paths which appear as sharding paths.
    std::vector<ProjectionNameOrderPreservingSet> _sargableProjectionNames;
};


/**
 * Logical property which specifies which collections (scanDefs) are available for a particular
 * group. For example if the group contains a join of two tables, we would have (at least) two
 * collections in the set.
 */
class CollectionAvailability final : public LogicalProperty {
public:
    CollectionAvailability(std::unordered_set<std::string> scanDefSet);

    bool operator==(const CollectionAvailability& other) const;

    const std::unordered_set<std::string>& getScanDefSet() const;

private:
    std::unordered_set<std::string> _scanDefSet;
};

struct DistributionHash {
    size_t operator()(const DistributionRequirement& distribution) const;
};

using DistributionSet = std::unordered_set<DistributionRequirement, DistributionHash>;

/**
 * Logical property which specifies promising projections and distributions to attempt to enforce
 * during physical optimization. For example, a group containing a GroupByNode would add hash
 * partitioning on the group-by projections.
 */
class DistributionAvailability final : public LogicalProperty {
public:
    DistributionAvailability(DistributionSet distributionSet);

    bool operator==(const DistributionAvailability& other) const;

    const DistributionSet& getDistributionSet() const;
    DistributionSet& getDistributionSet();

private:
    DistributionSet _distributionSet;
};

}  // namespace mongo::optimizer::properties
