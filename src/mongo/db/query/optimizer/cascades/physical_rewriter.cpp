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

#include "mongo/db/query/optimizer/cascades/physical_rewriter.h"
#include "mongo/db/query/optimizer/cascades/cost_derivation.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

using namespace properties;

template <class ToAddType, class ToRemoveType>
static void addRemoveProjectionsToProperties(Properties& properties,
                                             const ToAddType& toAdd,
                                             const ToRemoveType& toRemove) {
    ProjectionNameOrderPreservingSet& projections =
        getProperty<ProjectionRequirement>(properties).getProjections();
    for (const auto& varName : toRemove) {
        projections.erase(varName);
    }
    for (const auto& varName : toAdd) {
        projections.emplace_back(varName);
    }
}
template <class ToAddType>
static void addProjectionsToProperties(Properties& properties, const ToAddType& toAdd) {
    addRemoveProjectionsToProperties(properties, toAdd, ToAddType{});
}

static bool hasUnenforcedLimitSkip(const Properties& physProps) {
    return hasProperty<LimitSkipRequirement>(physProps) &&
        !getPropertyConst<LimitSkipRequirement>(physProps).isEnforced();
}

static bool isDistributionCentralizedOrReplicated(const Properties& physProps) {
    switch (getPropertyConst<DistributionRequirement>(physProps).getType()) {
        case DistributionType::Centralized:
        case DistributionType::Replicated:
            return true;

        default:
            return false;
    }
}

static ABT wrapConstFilter(ABT node) {
    return make<FilterNode>(Constant::boolean(true), std::move(node));
}

static ABT unwrapConstFilter(ABT node) {
    if (auto nodePtr = node.cast<FilterNode>();
        nodePtr != nullptr && nodePtr->getFilter() == Constant::boolean(true)) {
        return nodePtr->getChild();
    }
    return node;
}

template <class T>
static void optimizeChildren(PhysRewriteQueue& queue, ABT node, ChildPropsType childProps) {
    static_assert(canBePhysicalNode<T>(), "Can only optimize a physical node.");
    queue.emplace(std::make_unique<PhysRewriteEntry>(std::move(node), std::move(childProps)));
}

template <class T>
static void optimizeChild(PhysRewriteQueue& queue, ABT node, Properties childProps) {
    ABT& childRef = node.cast<T>()->getChild();
    optimizeChildren<T>(queue, std::move(node), ChildPropsType{{&childRef, std::move(childProps)}});
}

template <class T>
static void optimizeChild(PhysRewriteQueue& queue, ABT node) {
    optimizeChildren<T>(queue, std::move(node), {});
}

static void optimizeUnderNewProperties(PhysRewriteQueue& queue, ABT child, Properties props) {
    optimizeChild<FilterNode>(queue, wrapConstFilter(std::move(child)), std::move(props));
}

template <class P, class T>
static bool propertyAffectsProjections(const Properties& props, const T& projections) {
    if (!hasProperty<P>(props)) {
        return false;
    }

    const ProjectionNameSet& propProjections =
        getPropertyConst<P>(props).getAffectedProjectionNames();
    for (const ProjectionName& projectionName : projections) {
        if (propProjections.find(projectionName) != propProjections.cend()) {
            return true;
        }
    }

    return false;
}

template <class P>
static bool propertyAffectsProjection(const Properties& props,
                                      const ProjectionName& projectionName) {
    return propertyAffectsProjections<P>(props, ProjectionNameVector{projectionName});
}

template <class T>
static void optimizeChildren(PhysRewriteQueue& queue,
                             ABT node,
                             Properties leftProps,
                             Properties rightProps) {
    ABT& leftChildRef = node.cast<T>()->getLeftChild();
    ABT& rightChildRef = node.cast<T>()->getRightChild();
    optimizeChildren<T>(
        queue,
        std::move(node),
        {{&leftChildRef, std::move(leftProps)}, {&rightChildRef, std::move(rightProps)}});
}

/**
 * Checks if we are not trying to satisfy using the entire collection. We are either aiming for a
 * covered index, or for a seek.
 */
static bool hasIncompleteScanIndexingRequirement(const Properties& physProps) {
    return hasProperty<IndexingRequirement>(physProps) &&
        getPropertyConst<IndexingRequirement>(physProps).getIndexReqTarget() !=
        IndexReqTarget::Complete;
}

/**
 * Helper class used to check if two property sets are compatible by testing each constituent
 * property for compatibility.
 */
class PropCompatibleVisitor {
public:
    template <class T>
    bool operator()(const Property&, const T& requiredProp) {
        if constexpr (std::is_base_of_v<PhysicalProperty, T>) {
            return isPropertyCompatible(_availableProps, requiredProp);
        }
        return false;
    }

    static bool propertiesCompatible(const Properties& requiredProps,
                                     const Properties& availableProps) {
        PropCompatibleVisitor visitor(availableProps);
        for (const auto& entry : requiredProps) {
            if (!entry.second.visit(visitor)) {
                return false;
            }
        }
        return true;
    }

private:
    PropCompatibleVisitor(const Properties& availableProp) : _availableProps(availableProp) {}

    // We don't own this.
    const Properties& _availableProps;
};

class PropEnforcerVisitor {
public:
    template <class T>
    void operator()(const Property&, const T& prop) {
        static_assert(!std::is_base_of_v<PhysicalProperty, T>,
                      "Physical property must implement its enforcer visitor.");
    }

    void operator()(const Property&, const CollationRequirement& prop) {
        verifyPropertyProjections(prop);
        if (hasIncompleteScanIndexingRequirement(_physProps)) {
            // If we have indexing requirements, we do not enforce collation separately.
            // It will be satisfied as part of the index collation.
            return;
        }

        if (hasUnenforcedLimitSkip(_physProps)) {
            // Do not enforce collation if we have unenforced limit-skip.
            // TODO: consider semantics: we probably expect to be sorted first before limiting.
            return;
        }

        Properties childProps = _physProps;
        removeProperty<CollationRequirement>(childProps);
        addProjectionsToProperties(childProps, prop.getAffectedProjectionNames());
        // Collation is a blocking operation and subsequent executions can re-use the state cheaply.
        removeProperty<RepetitionEstimate>(childProps);

        ABT enforcer = make<CollationNode>(prop, make<MemoLogicalDelegatorNode>(_groupId));
        optimizeChild<CollationNode>(_queue, std::move(enforcer), std::move(childProps));
    }

    void operator()(const Property&, const LimitSkipRequirement& prop) {
        if (hasIncompleteScanIndexingRequirement(_physProps)) {
            // If we have indexing requirements, we do not enforce limit skip.
            return;
        }
        if (!isDistributionCentralizedOrReplicated(_physProps)) {
            // Can only enforce limit-skip under centralized or replicated distribution.
            return;
        }

        Properties childProps = _physProps;
        setPropertyOverwrite<LimitSkipRequirement>(
            childProps, LimitSkipRequirement{prop.getLimit(), prop.getSkip(), true /*isEnforced*/});

        ABT enforcer = make<LimitSkipNode>(prop, make<MemoLogicalDelegatorNode>(_groupId));
        optimizeChild<LimitSkipNode>(_queue, std::move(enforcer), std::move(childProps));
    }

    void operator()(const Property&, const DistributionRequirement& prop) {
        verifyPropertyProjections(prop);

        if (prop.getType() == DistributionType::UnknownPartitioning) {
            // Cannot exchange into unknown partitioning.
            return;
        }
        if (hasIncompleteScanIndexingRequirement(_physProps)) {
            // If we have indexing requirements, we do not enforce distribution.
            // TODO: handle indexes with distributions differing from their collection.
            return;
        }

        const bool hasCollation = hasProperty<CollationRequirement>(_physProps);
        if (hasCollation) {
            // For now we cannot enforce if we have collation requirement.
            // TODO: try enforcing into partitioning distributions which form prefixes over the
            // collation, with ordered exchange.
            return;
        }

        const auto& distributions =
            getPropertyConst<DistributionAvailability>(_logicalProps).getDistributionSet();
        for (const auto& distribution : distributions) {
            if (distribution == prop) {
                continue;
            }

            Properties childProps = _physProps;
            setPropertyOverwrite<DistributionRequirement>(childProps, distribution);

            addProjectionsToProperties(childProps, distribution.getAffectedProjectionNames());

            // TODO: consider using preserveSort under collation requirements.
            ABT enforcer = make<ExchangeNode>(
                prop, false /*preserveSort*/, make<MemoLogicalDelegatorNode>(_groupId));
            optimizeChild<ExchangeNode>(_queue, std::move(enforcer), std::move(childProps));
        }
    }

    void operator()(const Property&, const ProjectionRequirement& prop) {
        const ProjectionAvailability& availableProjections =
            getPropertyConst<ProjectionAvailability>(_logicalProps);

        // Verify we can satisfy the required projections using the logical projections.
        uassert(0, "Cannot satisfy all projections", availableProjections.canSatisfy(prop));
    }

    void operator()(const Property&, const IndexingRequirement& prop) {
        if (prop.getIndexReqTarget() != IndexReqTarget::Complete) {
            return;
        }

        uassert(0,
                "IndexingRequirement without indexing availability",
                hasProperty<IndexingAvailability>(_logicalProps));
        const auto& scanDefSet =
            getPropertyConst<CollectionAvailability>(_logicalProps).getScanDefSet();
        // TODO: consider ramifications for left outer joins. We can propagate rid from the otuer
        // side.
        uassert(0,
                "Indexing availability must imply a availability of exactly one collection",
                scanDefSet.size() == 1);
        if (_rewriter._metadata._scanDefs.at(*scanDefSet.begin()).getIndexDefs().empty()) {
            // No indexes on the collection.
            return;
        }

        const IndexingAvailability& indexingAvailability =
            getPropertyConst<IndexingAvailability>(_logicalProps);
        const ProjectionNameOrderPreservingSet& requiredProjections =
            getPropertyConst<ProjectionRequirement>(_physProps).getProjections();
        const ProjectionName& scanProjection = indexingAvailability.getScanProjection();
        const GroupIdType scanGroupId = indexingAvailability.getScanGroupId();
        const bool requiresScanProjection = requiredProjections.find(scanProjection).second;

        if (requiresScanProjection) {
            // Try removing indexing requirement by performing a seek on the inner side.
            ProjectionName ridProjectionName = _rewriter._ridPrefixId.getNextId("rid");

            ABT physicalJoin = make<BinaryJoinNode>(JoinType::Inner,
                                                    ProjectionNameSet{ridProjectionName},
                                                    Constant::boolean(true),
                                                    make<MemoLogicalDelegatorNode>(_groupId),
                                                    make<MemoLogicalDelegatorNode>(scanGroupId));

            ABT& leftChildRef = physicalJoin.cast<BinaryJoinNode>()->getLeftChild();
            ABT& rightChildRef = physicalJoin.cast<BinaryJoinNode>()->getRightChild();

            // For now we are propagating the distribution requirements on both sides.
            Properties leftProps = _physProps;
            setPropertyOverwrite<IndexingRequirement>(leftProps,
                                                      {IndexReqTarget::Index, ridProjectionName});
            getProperty<ProjectionRequirement>(leftProps).getProjections().erase(scanProjection);

            Properties rightProps = _physProps;
            // Add repeated execution property to inner side.
            CEType estimatedRepetitions = hasProperty<RepetitionEstimate>(_physProps)
                ? getPropertyConst<RepetitionEstimate>(_physProps).getEstimate()
                : 1.0;
            estimatedRepetitions *=
                getPropertyConst<CardinalityEstimate>(_logicalProps).getEstimate();
            setPropertyOverwrite<RepetitionEstimate>(rightProps,
                                                     RepetitionEstimate{estimatedRepetitions});

            setPropertyOverwrite<IndexingRequirement>(rightProps,
                                                      {IndexReqTarget::Seek, ridProjectionName});
            setPropertyOverwrite<ProjectionRequirement>(
                rightProps, ProjectionRequirement(ProjectionNameVector{scanProjection}));
            removeProperty<CollationRequirement>(rightProps);

            optimizeChildren<BinaryJoinNode>(
                _queue,
                std::move(physicalJoin),
                {{&leftChildRef, std::move(leftProps)}, {&rightChildRef, std::move(rightProps)}});
        } else {
            // Try indexScanOnly (covered index) if we do not require scan projection.
            Properties newProps = _physProps;
            setPropertyOverwrite<IndexingRequirement>(newProps, {IndexReqTarget::Index, ""});

            optimizeUnderNewProperties(
                _queue, make<MemoLogicalDelegatorNode>(_groupId), std::move(newProps));
        }
    }

    void operator()(const Property&, const RepetitionEstimate& prop) {
        // Noop. We do not currently enforce this property. It only affects costing.
        // TODO: consider materializing the subtree if we estimate a lot of repetitions.
    }

    static void addEnforcers(const GroupIdType groupId,
                             PhysicalRewriter& rewriter,
                             PhysRewriteQueue& queue,
                             const Properties& physProps,
                             const Properties& logicalProps) {
        PropEnforcerVisitor visitor(groupId, rewriter, queue, physProps, logicalProps);
        for (const auto& entry : physProps) {
            entry.second.visit(visitor);
        }
    }

private:
    PropEnforcerVisitor(const GroupIdType groupId,
                        PhysicalRewriter& rewriter,
                        PhysRewriteQueue& queue,
                        const Properties& physProps,
                        const Properties& logicalProps)
        : _groupId(groupId),
          _rewriter(rewriter),
          _queue(queue),
          _physProps(physProps),
          _logicalProps(logicalProps) {}

    template <class T>
    void verifyPropertyProjections(const T& prop) {
        // If we have a collation requirement, we must also require the individual projections.
        const auto& requiredProjections =
            getPropertyConst<ProjectionRequirement>(_physProps).getProjections();
        for (const ProjectionName& projectionName : prop.getAffectedProjectionNames()) {
            uassert(0,
                    "Property projection not amongst required projections",
                    requiredProjections.find(projectionName).second);
        }
    }

    const GroupIdType _groupId;

    // We don't own any of those.
    PhysicalRewriter& _rewriter;
    PhysRewriteQueue& _queue;
    const Properties& _physProps;
    const Properties& _logicalProps;
};

/**
 * Implement physical nodes based on existing logical nodes.
 */
class ImplementationVisitor {
public:
    void operator()(const ABT& /*n*/, const ScanNode& node) {
        if (hasUnenforcedLimitSkip(_physProps)) {
            // Cannot satisfy an unenforced limit-skip.
            return;
        }
        if (hasProperty<CollationRequirement>(_physProps)) {
            // Regular scan cannot satisfy any collation requirement.
            // TODO: consider rid?
            return;
        }

        const auto& indexReq = getPropertyConst<IndexingRequirement>(_physProps);
        switch (indexReq.getIndexReqTarget()) {
            case IndexReqTarget::Index:
                // At this point cannot only satisfy index-only.
                return;

            case IndexReqTarget::Seek:
                uassert(0,
                        "RID projection is required for seek",
                        !indexReq.getRIDProjectionName().empty());
                // Fall through to code below.
                break;

            case IndexReqTarget::Complete:
                // Fall through to code below.
                break;

            default:
                MONGO_UNREACHABLE;
        }

        // Handle complete indexing requirement.
        bool canUseParallelScan = false;
        if (!distributionsCompatible(
                _rewriter._metadata._scanDefs.at(node.getScanDefName()).getDistributionAndPaths(),
                node.getProjectionName(),
                _groupId,
                {},
                canUseParallelScan)) {
            return;
        }

        FieldProjectionMap fieldProjectionMap;
        for (const ProjectionName& required :
             getPropertyConst<ProjectionRequirement>(_physProps).getAffectedProjectionNames()) {
            if (required == node.getProjectionName()) {
                fieldProjectionMap._rootProjection = node.getProjectionName();
            } else {
                // Regular scan node can satisfy only using its root projection (not fields).
                return;
            }
        }

        if (indexReq.getIndexReqTarget() == IndexReqTarget::Seek) {
            ABT physicalSeek = make<SeekNode>(indexReq.getRIDProjectionName(),
                                              std::move(fieldProjectionMap),
                                              node.getScanDefName());
            optimizeChild<SeekNode>(_queue, std::move(physicalSeek));
        } else {
            if (!indexReq.getRIDProjectionName().empty()) {
                fieldProjectionMap._ridProjection = indexReq.getRIDProjectionName();
            }
            ABT physicalScan = make<PhysicalScanNode>(
                std::move(fieldProjectionMap), node.getScanDefName(), canUseParallelScan);
            optimizeChild<PhysicalScanNode>(_queue, std::move(physicalScan));
        }
    }

    void operator()(const ABT& /*n*/, const MemoLogicalDelegatorNode& /*node*/) {
        uasserted(6624000,
                  "Must not have logical delegator nodes in the list of the logical nodes");
    }

    void operator()(const ABT& n, const FilterNode& node) {
        // Add projections we depend on to the requirement.
        Properties newProps = _physProps;

        VariableNameSetType references = collectVariableReferences(n);
        if (checkIntroducesScanProjectionUnderIndexOnly(references)) {
            // Reject if under indexing requirements and now we introduce dependence on scan
            // projection.
            return;
        }

        addProjectionsToProperties(newProps, std::move(references));

        ABT physicalFilter = n;
        // For physical plans, do not restrict input projection, even if temporary.
        physicalFilter.cast<FilterNode>()->clearInputVarTemp();
        optimizeChild<FilterNode>(_queue, std::move(physicalFilter), std::move(newProps));
    }

    void operator()(const ABT& n, const EvaluationNode& node) {
        const ProjectionName& projectionName = node.getProjectionName();
        if (propertyAffectsProjection<DistributionRequirement>(_physProps, projectionName)) {
            // We cannot satisfy distribution on the projection we output.
            return;
        }
        if (propertyAffectsProjection<CollationRequirement>(_physProps, projectionName)) {
            // In general, we cannot satisfy collation on the projection we output.
            // TODO consider x = y+1, we can propagate the collation requirement from x to y.
            return;
        }
        if (!propertyAffectsProjection<ProjectionRequirement>(_physProps, projectionName)) {
            // We do not require the projection. Do not place a physical evaluation node and
            // continue optimizing the child.
            optimizeUnderNewProperties(_queue, node.getChild(), _physProps);
            return;
        }

        // Remove our projection from requirement, and add projections we depend on to the
        // requirement.
        Properties newProps = _physProps;

        VariableNameSetType references = collectVariableReferences(n);
        if (checkIntroducesScanProjectionUnderIndexOnly(references)) {
            // Reject if under indexing requirements and now we introduce dependence on scan
            // projection.
            return;
        }

        addRemoveProjectionsToProperties(
            newProps, std::move(references), ProjectionNameVector{projectionName});

        ABT physicalEval = n;
        optimizeChild<EvaluationNode>(_queue, std::move(physicalEval), std::move(newProps));
    }

    void operator()(const ABT& n, const SargableNode& node) {
        if (hasUnenforcedLimitSkip(_physProps)) {
            // Cannot satisfy an unenfoced limit-skip.
            return;
        }

        const IndexingAvailability& indexingAvailability =
            getPropertyConst<IndexingAvailability>(_logicalProps);
        if (node.getChild().cast<MemoLogicalDelegatorNode>()->getGroupId() !=
            indexingAvailability.getScanGroupId()) {
            // To optimize a sargable predicate, we must have the scan group as a child.
            return;
        }

        const auto& scanDefSet =
            getPropertyConst<CollectionAvailability>(_logicalProps).getScanDefSet();
        uassert(0,
                "Indexing availability must imply a availability of exactly one collection",
                scanDefSet.size() == 1);
        const std::string& scanDefName = *scanDefSet.begin();
        const auto& scanDef = _rewriter._metadata._scanDefs.at(scanDefName);


        // We do not check indexDefs to be empty here. We want to allow evaluations to be covered
        // via a physical scan even in the absence of indexes.

        const IndexingRequirement& requirements = getPropertyConst<IndexingRequirement>(_physProps);
        const IndexReqTarget indexReqTarget = requirements.getIndexReqTarget();
        const ProjectionName& ridProjectionName = requirements.getRIDProjectionName();
        const ProjectionName& scanProjectionName = indexingAvailability.getScanProjection();
        const GroupIdType scanGroupId = indexingAvailability.getScanGroupId();
        const PartialSchemaRequirements& reqMap = node.getReqMap();

        if (indexReqTarget != IndexReqTarget::Index &&
            hasProperty<CollationRequirement>(_physProps)) {
            // PhysicalScan or Seek cannot satisfy any collation requirement.
            // TODO: consider rid?
            return;
        }

        for (const auto& [key, req] : reqMap) {
            if (key.emptyPath()) {
                // We cannot satisfy without a field.
                return;
            }
            if (indexReqTarget != IndexReqTarget::Index &&
                req.getIntervals() != kIntervalReqFullyOpenDNF) {
                // Can satisfy intervals with indexes only.
                return;
            }
            if (key._projectionName != scanProjectionName) {
                // We can only satisfy partial schema requirements using our root projection.
                return;
            }
        }

        FieldProjectionMap fieldProjectionMap;
        bool hasProperPath = false;
        {
            ProjectionNameSet projectionsLeftToSatisfy =
                getPropertyConst<ProjectionRequirement>(_physProps).getAffectedProjectionNames();
            if (indexReqTarget != IndexReqTarget::Index) {
                // Deliver root projection if required.
                projectionsLeftToSatisfy.erase(scanProjectionName);
                fieldProjectionMap._rootProjection = scanProjectionName;
            }

            for (const auto& entry : reqMap) {
                if (entry.second.hasBoundProjectionName()) {
                    // Project field only if it required.
                    const ProjectionName& projectionName = entry.second.getBoundProjectionName();
                    projectionsLeftToSatisfy.erase(projectionName);

                    const FieldNameType fieldName = entry.first.getSimpleField();
                    if (fieldName.empty()) {
                        hasProperPath = true;
                    } else {
                        fieldProjectionMap._fieldProjections.emplace(fieldName, projectionName);
                    }
                }
            }
            if (!projectionsLeftToSatisfy.empty()) {
                // Unknown projections remain. Reject.
                return;
            }
        }

        if (indexReqTarget == IndexReqTarget::Index) {
            ProjectionCollationSpec requiredCollation;
            if (hasProperty<CollationRequirement>(_physProps)) {
                requiredCollation =
                    getPropertyConst<CollationRequirement>(_physProps).getCollationSpec();
            }

            // Consider all candidate indexes, and check if they satisfy the collation and
            // distribution requirements.
            for (const auto& [indexDefName, candidateIndexEntry] : node.getCandidateIndexMap()) {
                const auto& indexDef = scanDef.getIndexDefs().at(indexDefName);
                {
                    bool canUseParallelScanUnused = false;
                    if (!distributionsCompatible(indexDef.getDistributionAndPaths(),
                                                 scanProjectionName,
                                                 scanGroupId,
                                                 reqMap,
                                                 canUseParallelScanUnused)) {
                        return;
                    }
                }

                const auto availableDirections = indexSatisfiesCollation(
                    indexDef.getCollationSpec(), candidateIndexEntry, requiredCollation);
                if (!availableDirections._forward && !availableDirections._backward) {
                    // Failed to satisfy collation.
                    continue;
                }

                uassert(0,
                        "Either forward or backward direction must be available.",
                        availableDirections._forward || availableDirections._backward);

                auto indexProjectionMap = candidateIndexEntry._fieldProjectionMap;
                if (!ridProjectionName.empty()) {
                    indexProjectionMap._ridProjection = ridProjectionName;
                }

                ABT physicalIndexScan =
                    make<IndexScanNode>(std::move(indexProjectionMap),
                                        IndexSpecification{scanDefName,
                                                           indexDefName,
                                                           candidateIndexEntry._intervals,
                                                           !availableDirections._forward});

                optimizeChild<IndexScanNode>(_queue, std::move(physicalIndexScan));
            }
        } else if (!hasProperPath) {
            bool canUseParallelScan = false;
            if (!distributionsCompatible(scanDef.getDistributionAndPaths(),
                                         scanProjectionName,
                                         scanGroupId,
                                         reqMap,
                                         canUseParallelScan)) {
                return;
            }

            // PhysicalScans and Seeks can only cover single field names (no paths).
            if (ridProjectionName.empty()) {
                // No range. Return a physical scan with field map.
                ABT physicalScan = make<PhysicalScanNode>(
                    std::move(fieldProjectionMap), scanDefName, canUseParallelScan);
                optimizeChild<PhysicalScanNode>(_queue, std::move(physicalScan));
            } else {
                // Try Seek. "Limit 1" will be added implicitly.
                ABT physicalSeek =
                    make<SeekNode>(ridProjectionName, std::move(fieldProjectionMap), scanDefName);
                optimizeChild<SeekNode>(_queue, std::move(physicalSeek));
            }
        }
    }

    void operator()(const ABT& /*n*/, const RIDIntersectNode& node) {
        {
            const auto& scanDefSet =
                getPropertyConst<CollectionAvailability>(_logicalProps).getScanDefSet();
            uassert(0,
                    "Indexing availability must imply a availability of exactly one collection",
                    scanDefSet.size() == 1);
            const std::string& scanDefName = *scanDefSet.begin();
            const auto& scanDef = _rewriter._metadata._scanDefs.at(scanDefName);
            if (scanDef.getIndexDefs().empty()) {
                // Reject if we do not have any indexes.
                return;
            }
        }

        if (hasUnenforcedLimitSkip(_physProps)) {
            // Cannot satisfy an unenforced limit-skip.
            return;
        }

        const IndexingRequirement& requirements = getPropertyConst<IndexingRequirement>(_physProps);
        const IndexReqTarget indexReqTarget = requirements.getIndexReqTarget();
        const ProjectionName& reqRIDProjectionName = requirements.getRIDProjectionName();

        const ProjectionName& scanProjection =
            getPropertyConst<IndexingAvailability>(_logicalProps).getScanProjection();

        const GroupIdType leftGroupId =
            node.getLeftChild().cast<MemoLogicalDelegatorNode>()->getGroupId();
        const GroupIdType rightGroupId =
            node.getRightChild().cast<MemoLogicalDelegatorNode>()->getGroupId();

        const Properties& leftLogicalProps =
            _rewriter._memo.getGroup(leftGroupId)._logicalProperties;
        const Properties& rightLogicalProps =
            _rewriter._memo.getGroup(rightGroupId)._logicalProperties;

        const ProjectionNameSet& leftProjections =
            getPropertyConst<ProjectionAvailability>(leftLogicalProps).getProjections();
        const ProjectionNameSet& rightProjections =
            getPropertyConst<ProjectionAvailability>(rightLogicalProps).getProjections();

        ProjectionCollationSpec collationSpec;
        if (hasProperty<CollationRequirement>(_physProps)) {
            collationSpec = getPropertyConst<CollationRequirement>(_physProps).getCollationSpec();
        }

        // Split collation between inner and outer side.
        ProjectionCollationSpec leftCollationSpec;
        ProjectionCollationSpec rightCollationSpec;
        {
            bool validSplit = true;
            bool leftSide = true;
            for (const auto& collationEntry : collationSpec) {
                const ProjectionName& projectionName = collationEntry.first;

                if (leftProjections.count(projectionName) > 0) {
                    if (!leftSide) {
                        // Left and right projections must complement and form prefix and suffix.
                        validSplit = false;
                        break;
                    }
                    leftCollationSpec.push_back(collationEntry);
                } else if (rightProjections.count(projectionName) > 0) {
                    if (leftSide) {
                        leftSide = false;
                    }
                    rightCollationSpec.push_back(collationEntry);
                } else {
                    uasserted(0,
                              "Collation projection must appear in either the left or the right "
                              "child projections");
                    return;
                }
            }

            if (!validSplit) {
                return;
            }
        }

        // Split required projections between inner and outer side.
        ProjectionNameOrderPreservingSet leftChildProjections;
        ProjectionNameOrderPreservingSet rightChildProjections;
        {
            bool validSplit = true;
            for (const ProjectionName& projectionName :
                 getPropertyConst<ProjectionRequirement>(_physProps).getProjections().getVector()) {
                if (leftProjections.count(projectionName) > 0) {
                    if (projectionName == scanProjection) {
                        validSplit = false;
                        break;
                    }
                    leftChildProjections.emplace_back(projectionName);
                } else if (rightProjections.count(projectionName) > 0) {
                    if (indexReqTarget == IndexReqTarget::Index &&
                        projectionName == scanProjection) {
                        validSplit = false;
                        break;
                    }
                    rightChildProjections.emplace_back(projectionName);
                } else {
                    uasserted(
                        0,
                        "Required projection must appear in either the left or the right child "
                        "projections");
                    return;
                }
            }
            if (!validSplit) {
                return;
            }
        }

        // We are propagating the distribution requirements to both sides.
        Properties leftPhysProps = _physProps;
        Properties rightPhysProps = _physProps;

        // Specifically do not propagate limit-skip.
        // TODO: handle similarly to physical join.
        removeProperty<LimitSkipRequirement>(leftPhysProps);
        removeProperty<LimitSkipRequirement>(rightPhysProps);

        ProjectionName leftRIDProjectionName = reqRIDProjectionName.empty()
            ? _rewriter._ridPrefixId.getNextId("rid")
            : reqRIDProjectionName;
        ProjectionName rightRIDProjectionName = (indexReqTarget == IndexReqTarget::Index)
            ? _rewriter._ridPrefixId.getNextId("rid")
            : leftRIDProjectionName;

        setPropertyOverwrite<IndexingRequirement>(leftPhysProps,
                                                  {IndexReqTarget::Index, leftRIDProjectionName});
        setPropertyOverwrite<IndexingRequirement>(rightPhysProps,
                                                  {indexReqTarget,
                                                   (indexReqTarget == IndexReqTarget::Index)
                                                       ? rightRIDProjectionName
                                                       : leftRIDProjectionName});

        if (leftCollationSpec.empty()) {
            removeProperty<CollationRequirement>(leftPhysProps);
        } else {
            setPropertyOverwrite<CollationRequirement>(leftPhysProps, std::move(leftCollationSpec));
        }

        if (rightCollationSpec.empty()) {
            removeProperty<CollationRequirement>(rightPhysProps);
        } else {
            setPropertyOverwrite<CollationRequirement>(rightPhysProps,
                                                       std::move(rightCollationSpec));
        }

        setPropertyOverwrite<ProjectionRequirement>(leftPhysProps, std::move(leftChildProjections));
        setPropertyOverwrite<ProjectionRequirement>(rightPhysProps,
                                                    std::move(rightChildProjections));

        if (indexReqTarget == IndexReqTarget::Index) {
            // TODO: consider an optimization to use the smaller side (lower CE) as inner side.

            ABT physicalJoin = make<HashJoinNode>(JoinType::Inner,
                                                  ProjectionNameVector{leftRIDProjectionName},
                                                  ProjectionNameVector{rightRIDProjectionName},
                                                  make<MemoLogicalDelegatorNode>(leftGroupId),
                                                  make<MemoLogicalDelegatorNode>(rightGroupId));

            optimizeChildren<HashJoinNode>(_queue,
                                           std::move(physicalJoin),
                                           std::move(leftPhysProps),
                                           std::move(rightPhysProps));
        } else {
            // Add repeated execution property to inner side.
            CEType estimatedRepetitions = hasProperty<RepetitionEstimate>(_physProps)
                ? getPropertyConst<RepetitionEstimate>(_physProps).getEstimate()
                : 1.0;
            estimatedRepetitions *=
                getPropertyConst<CardinalityEstimate>(leftLogicalProps).getEstimate();
            setPropertyOverwrite<RepetitionEstimate>(rightPhysProps,
                                                     RepetitionEstimate{estimatedRepetitions});

            ABT physicalJoin = make<BinaryJoinNode>(JoinType::Inner,
                                                    ProjectionNameSet{leftRIDProjectionName},
                                                    Constant::boolean(true),
                                                    make<MemoLogicalDelegatorNode>(leftGroupId),
                                                    make<MemoLogicalDelegatorNode>(rightGroupId));

            optimizeChildren<BinaryJoinNode>(_queue,
                                             std::move(physicalJoin),
                                             std::move(leftPhysProps),
                                             std::move(rightPhysProps));
        }
    }

    void operator()(const ABT& /*n*/, const BinaryJoinNode& node) {
        // TODO: optimize binary joins
        uasserted(0, "not implemented");
    }

    void operator()(const ABT& /*n*/, const InnerMultiJoinNode& node) {
        // TODO: optimize multi-joins
        uasserted(0, "not implemented");
    }

    void operator()(const ABT& /*n*/, const UnionNode& node) {
        // TODO: optimize union.
        uasserted(0, "not implemented");
    }

    void operator()(const ABT& n, const GroupByNode& node) {
        if (hasUnenforcedLimitSkip(_physProps)) {
            // We cannot satisfy limit-skip requirements.
            // TODO: consider an optimization where we keep track of at most "limit" groups.
            return;
        }
        if (hasProperty<CollationRequirement>(_physProps)) {
            // In general we cannot satisfy collation requirements.
            // TODO: consider stream group-by.
            return;
        }

        if (propertyAffectsProjections<DistributionRequirement>(
                _physProps, node.getAggregationProjectionNames())) {
            // We cannot satisfy distribution on the aggregations.
            return;
        }

        if (!node.isLocal()) {
            // We are constrained in terms of distribution only if we are a global agg.

            const ProjectionNameVector& groupByProjections = node.getGroupByProjectionNames();
            const auto& requiredDistribution =
                getPropertyConst<DistributionRequirement>(_physProps);

            switch (requiredDistribution.getType()) {
                case DistributionType::UnknownPartitioning:
                    // Cannot satisfy unknown partitioning.
                    return;

                case DistributionType::HashPartitioning: {
                    ProjectionNameSet groupByProjectionSet;
                    for (const ProjectionName& projectionName : groupByProjections) {
                        groupByProjectionSet.insert(projectionName);
                    }
                    for (const ProjectionName& projectionName :
                         requiredDistribution.getProjections()) {
                        if (groupByProjectionSet.count(projectionName) == 0) {
                            // We can only be partitioned on projections on which we group.
                            return;
                        }
                    }
                    break;
                }

                case DistributionType::RangePartitioning:
                    if (requiredDistribution.getProjections() != groupByProjections) {
                        // For range partitioning we need to be partitioned exactly in the same
                        // order as our group-by projections.
                        return;
                    }
                    break;

                default:
                    break;
            }
        }

        Properties newProps = _physProps;
        // Hash group-by is a blocking operation and subsequent executions can re-use the state
        // cheaply.
        // TODO: this is not the case for stream group-by.
        removeProperty<RepetitionEstimate>(newProps);

        // Specifically do not propagate limit-skip.
        removeProperty<LimitSkipRequirement>(newProps);

        // Iterate over the aggregation expressions and only add those required.
        ABTVector aggregationProjections;
        ProjectionNameVector aggregationProjectionNames;
        VariableNameSetType projectionsToAdd;
        const auto& requiredProjections =
            getPropertyConst<ProjectionRequirement>(_physProps).getProjections();
        for (size_t aggIndex = 0; aggIndex < node.getAggregationExpressions().size(); aggIndex++) {
            const ProjectionName& aggProjectionName =
                node.getAggregationProjectionNames().at(aggIndex);

            if (requiredProjections.find(aggProjectionName).second) {
                // We require this agg expression.
                aggregationProjectionNames.push_back(aggProjectionName);
                const ABT& aggExpr = node.getAggregationExpressions().at(aggIndex);
                aggregationProjections.push_back(aggExpr);

                for (const auto& var : VariableEnvironment::getVariables(aggExpr)._variables) {
                    // Add all references this expression requires.
                    projectionsToAdd.insert(var->name());
                }
            }
        }

        addRemoveProjectionsToProperties(
            newProps, projectionsToAdd, node.getAggregationProjectionNames());

        ABT physicalGroupBy = make<GroupByNode>(node.getGroupByProjectionNames(),
                                                std::move(aggregationProjectionNames),
                                                std::move(aggregationProjections),
                                                node.isLocal(),
                                                node.canRewriteIntoLocal(),
                                                node.getChild());
        optimizeChild<GroupByNode>(_queue, std::move(physicalGroupBy), std::move(newProps));
    }

    void operator()(const ABT& n, const UnwindNode& node) {
        const ProjectionName& pidProjectionName = node.getPIDProjectionName();
        const ProjectionNameVector& projectionNames = {(node.getProjectionName()),
                                                       pidProjectionName};

        if (propertyAffectsProjections<DistributionRequirement>(_physProps, projectionNames)) {
            // We cannot satisfy distribution on the unwound output, or pid.
            return;
        }
        if (propertyAffectsProjections<CollationRequirement>(_physProps, projectionNames)) {
            // We cannot satisfy collation on the output.
            return;
        }
        if (hasUnenforcedLimitSkip(_physProps)) {
            // Cannot satisfy limit-skip.
            return;
        }

        Properties newProps = _physProps;
        addRemoveProjectionsToProperties(
            newProps, collectVariableReferences(n), ProjectionNameVector{pidProjectionName});

        // Specifically do not propagate limit-skip.
        removeProperty<LimitSkipRequirement>(newProps);
        // Keep collation property if given it does not affect output.

        ABT physicalUnwind = n;
        optimizeChild<UnwindNode>(_queue, std::move(physicalUnwind), std::move(newProps));
    }

    void operator()(const ABT& /*n*/, const WindNode& node) {
        // TODO: optimize wind.
        uasserted(0, "not implemented");
    }

    void operator()(const ABT& /*n*/, const CollationNode& node) {
        if (getPropertyConst<DistributionRequirement>(_physProps).getType() !=
            DistributionType::Centralized) {
            // We can only pick up collation under centralized (but we can enforce under any
            // distribution).
            return;
        }

        optimizeSimplePropertyNode<CollationNode, CollationRequirement>(node);
    }

    void operator()(const ABT& /*n*/, const LimitSkipNode& node) {
        // We can pick-up limit-skip under any distribution (but enforce under centralized or
        // replicated).

        Properties newProps = _physProps;
        LimitSkipRequirement newProp = node.getProperty();

        if (hasProperty<LimitSkipRequirement>(_physProps)) {
            const LimitSkipRequirement& required =
                getPropertyConst<LimitSkipRequirement>(_physProps);
            LimitSkipRequirement merged(required.getLimit(), required.getSkip());

            if (merged.mergeWith(newProp)) {
                if (required.isEnforced() && required.getLimit() == merged.getLimit() &&
                    required.getSkip() == merged.getSkip()) {
                    // Current node does not impose more constraints which arent already enforced.
                    // Continue with existing enforced requirement.
                    newProp = required;
                } else {
                    // Continue with new unenforced requirement.
                    newProp = std::move(merged);
                }
            }
        }

        setPropertyOverwrite<LimitSkipRequirement>(newProps, std::move(newProp));
        optimizeUnderNewProperties(_queue, node.getChild(), std::move(newProps));
    }

    void operator()(const ABT& /*n*/, const ExchangeNode& node) {
        optimizeSimplePropertyNode<ExchangeNode, DistributionRequirement>(node);
    }

    void operator()(const ABT& n, const RootNode& node) {
        Properties newProps = _physProps;
        setPropertyOverwrite<ProjectionRequirement>(newProps, node.getProperty());

        ABT rootNode = n;
        optimizeChild<RootNode>(_queue, std::move(rootNode), std::move(newProps));
    }

    template <typename T>
    void operator()(const ABT& /*n*/, const T& /*node*/) {
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its visitor.");
    }

    ImplementationVisitor(const GroupIdType groupId,
                          PhysicalRewriter& rewriter,
                          PhysRewriteQueue& queue,
                          const Properties& physProps,
                          const Properties& logicalProps)
        : _groupId(groupId),
          _rewriter(rewriter),
          _queue(queue),
          _physProps(physProps),
          _logicalProps(logicalProps) {}

private:
    template <class NodeType, class PropType>
    void optimizeSimplePropertyNode(const NodeType& node) {
        const PropType& nodeProp = node.getProperty();
        Properties newProps = _physProps;
        setPropertyOverwrite<PropType>(newProps, nodeProp);

        addProjectionsToProperties(newProps, nodeProp.getAffectedProjectionNames());

        optimizeUnderNewProperties(_queue, node.getChild(), std::move(newProps));
    }

    struct IndexAvailableDirections {
        // Keep track if we can match against forward or backward direction.
        bool _forward = true;
        bool _backward = true;
    };

    IndexAvailableDirections indexSatisfiesCollation(
        const IndexCollationSpec& indexCollationSpec,
        const CandidateIndexEntry& candidateIndexEntry,
        const ProjectionCollationSpec& requiredCollationSpec) {
        IndexAvailableDirections result;
        size_t collationSpecIndex = 0;
        bool indexSuitable = true;
        const auto& fieldProjections = candidateIndexEntry._fieldProjectionMap._fieldProjections;

        // Index selection loop. Iterate through all indexes. Verify they are compatible with
        // our collation requirement, and can deliver the right order of paths. For each
        // compatible index, generate an index scan node.
        for (size_t indexField = 0; indexField < indexCollationSpec.size(); indexField++) {
            auto it = fieldProjections.find(computeIndexKeyName(indexField));
            if (it == fieldProjections.cend()) {
                // No bound projection for this index;
                continue;
            }

            if (collationSpecIndex < requiredCollationSpec.size()) {
                // Check if we can satisfy the next collation requirement.
                const auto& collationEntry = requiredCollationSpec.at(collationSpecIndex);
                if (collationEntry.first != it->second) {
                    indexSuitable = false;
                    break;
                }

                if (candidateIndexEntry._fieldsToCollate.count(indexField) > 0) {
                    const auto& indexCollationEntry = indexCollationSpec.at(indexField);
                    if (result._forward &&
                        !collationOpsCompatible(indexCollationEntry._op, collationEntry.second)) {
                        result._forward = false;
                    }
                    if (result._backward &&
                        !collationOpsCompatible(reverseCollationOp(indexCollationEntry._op),
                                                collationEntry.second)) {
                        result._backward = false;
                    }
                    if (!result._forward && !result._backward) {
                        indexSuitable = false;
                        break;
                    }
                }

                collationSpecIndex++;
            }
        }

        if (!indexSuitable || collationSpecIndex < requiredCollationSpec.size()) {
            return {false, false};
        }
        return result;
    }

    /**
     * Check if we are under index-only requirements and expression introduces dependency on scan
     * projection.
     */
    bool checkIntroducesScanProjectionUnderIndexOnly(const VariableNameSetType& references) {
        return hasProperty<IndexingAvailability>(_logicalProps) &&
            getPropertyConst<IndexingRequirement>(_physProps).getIndexReqTarget() ==
            IndexReqTarget::Index &&
            references.find(
                getPropertyConst<IndexingAvailability>(_logicalProps).getScanProjection()) !=
            references.cend();
    }

    bool distributionsCompatible(const DistributionAndPaths& distributionAndPaths,
                                 const ProjectionName& scanProjection,
                                 const GroupIdType scanGroupId,
                                 const PartialSchemaRequirements& reqMap,
                                 bool& canUseParallelScan) {
        const DistributionRequirement& required =
            getPropertyConst<DistributionRequirement>(_physProps);
        const auto& scanLogicalProps = _rewriter._memo.getGroup(scanGroupId)._logicalProperties;
        const auto& scanDistributions =
            getPropertyConst<DistributionAvailability>(scanLogicalProps).getDistributionSet();

        switch (required.getType()) {
            case DistributionType::Centralized:
                return scanDistributions.count({DistributionType::Centralized}) > 0 ||
                    scanDistributions.count({DistributionType::Replicated}) > 0;

            case DistributionType::Replicated:
                return scanDistributions.count({DistributionType::Replicated}) > 0;

            case DistributionType::RoundRobin:
                // TODO: Are two round robin distributions compatible?
                return false;

            case DistributionType::UnknownPartitioning:
                if (scanDistributions.count({DistributionType::UnknownPartitioning}) > 0) {
                    canUseParallelScan = true;
                    return true;
                }
                return false;

            case DistributionType::HashPartitioning:
            case DistributionType::RangePartitioning: {
                if (required.getType() != distributionAndPaths._type) {
                    return false;
                }

                size_t distributionPartitionIndex = 0;
                const ProjectionNameVector& requiredProjections = required.getProjections();

                for (const ABT& partitioningPath : distributionAndPaths._paths) {
                    auto it = reqMap.find(PartialSchemaKey{scanProjection, partitioningPath});
                    if (it == reqMap.cend()) {
                        return false;
                    }

                    if (it->second.getBoundProjectionName() !=
                        requiredProjections.at(distributionPartitionIndex)) {
                        return false;
                    }
                    distributionPartitionIndex++;
                }

                return distributionPartitionIndex == requiredProjections.size();
            }

            default:
                MONGO_UNREACHABLE;
        }
    }

    const GroupIdType _groupId;

    // We don't own any of those;
    PhysicalRewriter& _rewriter;
    PhysRewriteQueue& _queue;
    const Properties& _physProps;
    const Properties& _logicalProps;
};

PhysicalRewriter::PhysicalRewriter(Memo& memo, const Metadata& metadata)
    : PhysicalRewriter(memo, metadata, deriveCost) {}

PhysicalRewriter::PhysicalRewriter(Memo& memo,
                                   const Metadata& metadata,
                                   const CostEstimationFn& costFn)
    : _planExplorationCount(0), _memo(memo), _costFn(costFn), _metadata(metadata) {}

static void printCandidateInfo(const ABT& node,
                               const GroupIdType groupId,
                               const CostType nodeCost,
                               const ChildPropsType& childProps,
                               const PhysOptimizationResult& bestResult) {
    std::cout << "group: " << groupId << ", id: " << bestResult._index
              << ", nodeCost: " << nodeCost.toString()
              << ", best cost: " << bestResult._cost.toString() << "\n";
    std::cout << ExplainGenerator::explainProperties("Physical properties", bestResult._physProps)
              << "\n";
    std::cout << "Node: " << ExplainGenerator::explainV2(node) << "\n";

    for (const auto& childProp : childProps) {
        std::cout << ExplainGenerator::explainProperties("Child properties", childProp.second);
    }
}

void PhysicalRewriter::costAndRetainBestNode(ABT node,
                                             ChildPropsType childProps,
                                             const GroupIdType groupId,
                                             const Properties& logicalProps,
                                             const PrefixId& prefixId,
                                             PhysOptimizationResult& bestResult) {
    const CostType nodeCost =
        _costFn(_metadata, _memo, logicalProps, bestResult._physProps, node.ref());
    uassert(
        6624000, "Must get non-infinity cost for physical node.", nodeCost != CostType::kInfinity);

    if (_memo.getDebugInfo().hasDebugLevel(3)) {
        std::cout << "Requesting optimization\n";
        printCandidateInfo(node, groupId, nodeCost, childProps, bestResult);
    }

    const CostType childCostLimit =
        bestResult.hasResult() ? bestResult._cost : bestResult._costLimit;
    const auto [success, cost] =
        optimizeChildren(nodeCost, std::move(childProps), prefixId, childCostLimit);
    const bool improvement = success && (cost < bestResult._cost);

    if (_memo.getDebugInfo().hasDebugLevel(3)) {
        std::cout << (success ? (improvement ? "Improved" : "Dis not improve")
                              : "Failed optimizing");
        std::cout << "Requesting optimization\n";
        printCandidateInfo(node, groupId, nodeCost, childProps, bestResult);
    }

    if (improvement) {
        bestResult._cost = cost;
        bestResult._node = unwrapConstFilter(std::move(node));
    }
}

/**
 * Convert nodes from logical to physical memo delegators.
 * Performs branch-and-bound search.
 */
std::pair<bool, CostType> PhysicalRewriter::optimizeChildren(const CostType nodeCost,
                                                             ChildPropsType childProps,
                                                             const PrefixId& prefixId,
                                                             const CostType costLimit) {
    CostType totalCost = nodeCost;
    if (costLimit < totalCost) {
        return {false, CostType::kInfinity};
    }

    for (auto& entry : childProps) {
        const GroupIdType groupId = entry.first->cast<MemoLogicalDelegatorNode>()->getGroupId();

        CostType childCostLimit = costLimit - totalCost;
        auto optGroupResult = optimizeGroup(groupId, entry.second, prefixId, childCostLimit);
        if (!optGroupResult._success) {
            return {false, CostType::kInfinity};
        }

        totalCost += optGroupResult._cost;
        if (costLimit < totalCost) {
            return {false, CostType::kInfinity};
        }

        ABT optimizedChild =
            make<MemoPhysicalDelegatorNode>(MemoPhysicalNodeId{groupId, optGroupResult._index});
        std::swap(*entry.first, optimizedChild);
    }

    return {true, totalCost};
}

PhysicalRewriter::OptimizeGroupResult PhysicalRewriter::applyRewrites(
    const GroupIdType groupId,
    const size_t localPlanExplorationCount,
    const Group& group,
    PrefixId& prefixId,
    PhysOptimizationResult& bestResult) {
    // Perform branch-and-bound optimization.
    while (!bestResult._queue.empty()) {
        PhysRewriteEntry rewrite = std::move(*bestResult._queue.front());
        bestResult._queue.pop();

        costAndRetainBestNode(std::move(rewrite._node),
                              std::move(rewrite._childProps),
                              groupId,
                              group._logicalProperties,
                              prefixId,
                              bestResult);
    }

    uassert(0, "Result is not optimized!", bestResult.isOptimized());
    if (!bestResult.hasResult()) {
        return {};
    }

    // We have a successful rewrite.
    if (_memo.getDebugInfo().hasDebugLevel(2)) {
        std::cout << "#" << localPlanExplorationCount << " Optimized group: " << groupId
                  << ", id: " << bestResult._index << ", cost: " << bestResult._cost.toString()
                  << "\n";
        std::cout << ExplainGenerator::explainProperties("Physical properties",
                                                         bestResult._physProps)
                  << "\n";
        std::cout << "Node: "
                  << ExplainGenerator::explainV2(
                         group._physicalNodes.at(bestResult._index)->_node, false, &_memo);
    }

    return {bestResult._index, bestResult._cost};
}

PhysicalRewriter::OptimizeGroupResult::OptimizeGroupResult()
    : _success(false), _index(0), _cost(CostType::kInfinity) {}

PhysicalRewriter::OptimizeGroupResult::OptimizeGroupResult(const size_t index, const CostType cost)
    : _success(true), _index(index), _cost(std::move(cost)) {
    uassert(
        0, "Cannot have successful optimization with infinite cost", _cost < CostType::kInfinity);
}

PhysicalRewriter::OptimizeGroupResult PhysicalRewriter::optimizeGroup(const GroupIdType groupId,
                                                                      const Properties& physProps,
                                                                      PrefixId prefixId,
                                                                      CostType costLimit) {
    const size_t localPlanExplorationCount = ++_planExplorationCount;
    if (_memo.getDebugInfo().hasDebugLevel(2)) {
        std::cout << "#" << localPlanExplorationCount << " Optimizing group " << groupId
                  << ", cost limit: " << costLimit.toString() << "\n";
        std::cout << ExplainGenerator::explainProperties("Physical properties", physProps) << "\n";
    }

    // If true, we have found compatible (but not equal) props with cost under our cost limit.
    bool hasCompatibleProps = false;

    Group& group = _memo.getGroup(groupId);
    const Properties& logicalProps = group._logicalProperties;
    if (hasProperty<IndexingAvailability>(logicalProps) &&
        !hasProperty<IndexingRequirement>(physProps)) {
        // Re-optimize under complete scan indexing requirements.
        Properties newProps = physProps;
        setPropertyOverwrite<IndexingRequirement>(newProps, {});
        return optimizeGroup(groupId, std::move(newProps), prefixId, costLimit);
    }

    // Check winner's circle for compatible properties.
    for (const auto& optimized : group._physicalNodes) {
        const bool hasPartialResult = optimized->hasResult();

        if (physProps == optimized->_physProps) {
            if (!optimized->isOptimized()) {
                // Currently optimizing under same properties. Continue optimization with remaining
                // rules.
                return applyRewrites(
                    groupId, localPlanExplorationCount, group, prefixId, *optimized);
            }
            // At this point we have an optimized entry.

            if (!hasPartialResult) {
                return {};
            }

            if (costLimit < optimized->_cost) {
                // We have a stricter limit than our previous optimization's cost.
                return {};
            }

            // Reuse result under identical properties.
            if (_memo.getDebugInfo().hasDebugLevel(3)) {
                std::cout << "Reusing winner's circle entry: group: " << groupId
                          << ", id: " << optimized->_index
                          << ", cost: " << optimized->_cost.toString()
                          << ", limit: " << costLimit.toString() << "\n";
                std::cout << "Existing props: "
                          << ExplainGenerator::explainProperties("existing", optimized->_physProps)
                          << "\n";
                std::cout << "New props: " << ExplainGenerator::explainProperties("new", physProps)
                          << "\n";
                std::cout << "Reused plan: " << ExplainGenerator::explainV2(optimized->_node)
                          << "\n";
            }

            return {optimized->_index, optimized->_cost};
        }

        if (!hasPartialResult) {
            continue;
        }
        // At this point we have an optimized entry.

        if (costLimit < optimized->_cost) {
            // Properties are not identical. Continue exploring even if limit was stricter.
            continue;
        }

        if (hasProperty<IndexingRequirement>(optimized->_physProps) !=
            hasProperty<IndexingRequirement>(physProps)) {
            // Do not reuse plan with indexing requirements if not under indexing requirements and
            // vice versa.
            continue;
        }

        if (!PropCompatibleVisitor::propertiesCompatible(physProps, optimized->_physProps)) {
            // We are more strict that what is available.
            continue;
        }

        if (optimized->_cost < costLimit) {
            if (_memo.getDebugInfo().hasDebugLevel(3)) {
                std::cout << "Reducing cost limit: group: " << groupId
                          << ", id: " << optimized->_index
                          << ", cost: " << optimized->_cost.toString()
                          << ", limit: " << costLimit.toString() << "\n";
                std::cout << "Existing props: "
                          << ExplainGenerator::explainProperties("existing", optimized->_physProps)
                          << "\n";
                std::cout << "New props: " << ExplainGenerator::explainProperties("new", physProps)
                          << "\n";
            }

            // Reduce cost limit result under compatible properties.
            hasCompatibleProps = true;
            costLimit = optimized->_cost;
        }
    }

    if (hasCompatibleProps) {
        auto result = optimizeGroup(groupId, physProps, prefixId, costLimit);
        if (!result._success) {
            uasserted(0, "We must be able to optimize under less restrictions");
        }
        return result;
    }

    // Populate with a new entry to mark optimization in progress for the current properties.
    // Refer to current best result from winner's circle.
    PhysOptimizationResult& bestResult = group.addOptimizationResult(physProps, costLimit);

    if (hasProperty<ProjectionRequirement>(bestResult._physProps)) {
        // Verify properties can be enforced and add enforcers if necessary.
        PropEnforcerVisitor::addEnforcers(
            groupId, *this, bestResult._queue, bestResult._physProps, logicalProps);
    }

    // Add rewrites to convert logical into physical nodes.
    ImplementationVisitor visitor(
        groupId, *this, bestResult._queue, bestResult._physProps, logicalProps);
    for (const ABT& node : group._logicalNodes.getVector()) {
        node.visit(visitor);
    }

    return applyRewrites(groupId, localPlanExplorationCount, group, prefixId, bestResult);
}

size_t PhysicalRewriter::getPlanExplorationCount() const {
    return _planExplorationCount;
}

}  // namespace mongo::optimizer::cascades
