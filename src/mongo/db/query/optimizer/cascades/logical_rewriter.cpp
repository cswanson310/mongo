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

#include "mongo/db/query/optimizer/cascades/logical_rewriter.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

LogicalRewriter::RewriteSet LogicalRewriter::_rewriteSet = {
    {LogicalRewriteType::FilterFilterReorder, 1},
    {LogicalRewriteType::EvaluationEvaluationReorder, 1},
    {LogicalRewriteType::FilterEvaluationReorder, 1},
    {LogicalRewriteType::EvaluationFilterReorder, 1},
    {LogicalRewriteType::FilterCollationReorder, 1},
    {LogicalRewriteType::EvaluationCollationReorder, 1},
    {LogicalRewriteType::FilterLimitSkipReorder, 1},
    {LogicalRewriteType::EvaluationLimitSkipReorder, 1},

    {LogicalRewriteType::FilterGroupByReorder, 1},
    {LogicalRewriteType::GroupCollationReorder, 1},

    {LogicalRewriteType::FilterUnwindReorder, 1},
    {LogicalRewriteType::EvaluationUnwindReorder, 1},
    {LogicalRewriteType::UnwindCollationReorder, 1},
    {LogicalRewriteType::UnwindLimitSkipReorder, 1},

    {LogicalRewriteType::FilterExchangeReorder, 1},
    {LogicalRewriteType::ExchangeEvaluationReorder, 1},
    {LogicalRewriteType::ExchangeCollationReorder, 1},
    {LogicalRewriteType::ExchangeLimitSkipReorder, 1},

    {LogicalRewriteType::CollationMerge, 1},
    {LogicalRewriteType::LimitSkipMerge, 1},

    {LogicalRewriteType::GroupByLocalGlobal, 1},

    {LogicalRewriteType::SargableFilterConvert, 2},
    {LogicalRewriteType::SargableEvaluationConvert, 2},
    {LogicalRewriteType::SargableMerge, 2},
    {LogicalRewriteType::SargableSplit, 3}};

bool LogicalRewriter::RewriteEntry::operator<(const LogicalRewriter::RewriteEntry& other) const {
    // Lower numerical priority is considered first.
    // Make sure entries in the queue are consistently ordered.

    if (other._priority < _priority) {
        return true;
    } else if (other._priority > _priority) {
        return false;
    }

    if (_nodeId.first < other._nodeId.first) {
        return true;
    } else if (_nodeId.first > other._nodeId.first) {
        return false;
    }

    return _nodeId.second < other._nodeId.second;
}

LogicalRewriter::LogicalRewriter(Memo& memo,
                                 PrefixId& prefixId,
                                 const Metadata& metadata,
                                 RewriteSet rewriteSet)
    : _activeRewriteSet(std::move(rewriteSet)),
      _queue(),
      _memo(memo),
      _prefixId(prefixId),
      _metadata(metadata) {}

GroupIdType LogicalRewriter::addInitialNode(const ABT& node) {
    return addNode(node, -1);
}

GroupIdType LogicalRewriter::addNode(const ABT& node, const GroupIdType targetGroupId) {
    Memo::NodeIdSet insertNodeIds;

    Memo::NodeTargetGroupMap targetGroupMap;
    if (targetGroupId >= 0) {
        targetGroupMap = {{node.ref(), targetGroupId}};
    }

    const GroupIdType resultGroupId =
        _memo.integrate(node, std::move(targetGroupMap), insertNodeIds);

    uassert(6624000,
            "Result group is not the same as target group",
            targetGroupId < 0 || targetGroupId == resultGroupId);

    for (const Memo::NodeId& nodeMemoId : insertNodeIds) {
        for (const auto& entry : _rewriteSet) {
            _queue.push({entry.first, entry.second, nodeMemoId});
        }
    }

    return resultGroupId;
}

struct RewriteContext {
    RewriteContext(LogicalRewriter& rewriter, const GroupIdType groupId)
        : _groupId(groupId), _rewriter(rewriter){};

    void addNode(const ABT& node) {
        _rewriter.addNode(node, _groupId);
    }

    Memo& getMemo() const {
        return _rewriter._memo;
    }

    const Metadata& getMetadata() const {
        return _rewriter._metadata;
    }

    PrefixId& getPrefixId() const {
        return _rewriter._prefixId;
    }

    const properties::Properties& getAboveLogicalProps() const {
        return getMemo().getGroup(_groupId)._logicalProperties;
    }

    const GroupIdType _groupId;

    // We don't own this.
    LogicalRewriter& _rewriter;
};

struct ReorderDependencies {
    bool _hasNodeRef = false;
    bool _hasChildRef = false;
    bool _hasNodeAndChildRef = false;
};

template <class AboveType, class BelowType>
ReorderDependencies computeDependencies(ABT::reference_type aboveNodeRef,
                                        ABT::reference_type belowNodeRef,
                                        RewriteContext& ctx) {
    // Get variables from above node and check if they are bound at below node, or at below node's
    // child.
    const auto aboveNodeVarNames = collectVariableReferences(aboveNodeRef);

    ABT belowNode = belowNodeRef;
    VariableEnvironment env = VariableEnvironment::build(belowNode, &ctx.getMemo());
    const DefinitionsMap belowNodeDefs = env.hasDefinitions(belowNode.ref())
        ? env.getDefinitions(belowNode.ref())
        : DefinitionsMap{};
    ABT::reference_type belowChild = belowNode.cast<BelowType>()->getChild().ref();
    const DefinitionsMap belowChildNodeDefs =
        env.hasDefinitions(belowChild) ? env.getDefinitions(belowChild) : DefinitionsMap{};

    ReorderDependencies dependencies;
    for (const std::string& varName : aboveNodeVarNames) {
        auto it = belowNodeDefs.find(varName);
        // Variable is exclusively defined in the below node.
        const bool refersToNode = it != belowNodeDefs.cend() && it->second.definedBy == belowNode;
        // Variable is defined in the belowNode's child subtree.
        const bool refersToChild = belowChildNodeDefs.find(varName) != belowChildNodeDefs.cend();

        if (refersToNode) {
            if (refersToChild) {
                dependencies._hasNodeAndChildRef = true;
            } else {
                dependencies._hasNodeRef = true;
            }
        } else if (refersToChild) {
            dependencies._hasChildRef = true;
        } else {
            // Lambda variable. Ignore.
        }
    }

    return dependencies;
}

template <class AboveType, class BelowType>
void defaultReorder(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) {
    ABT newParent = belowNode;
    ABT newChild = aboveNode;

    std::swap(newParent.cast<BelowType>()->getChild(), newChild.cast<AboveType>()->getChild());
    newParent.cast<BelowType>()->getChild() = std::move(newChild);

    ctx.addNode(newParent);
}

template <class AboveType, class BelowType>
void defaultReorderWithDependenceCheck(ABT::reference_type aboveNode,
                                       ABT::reference_type belowNode,
                                       RewriteContext& ctx) {
    const ReorderDependencies dependencies =
        computeDependencies<AboveType, BelowType>(aboveNode, belowNode, ctx);
    if (dependencies._hasNodeRef) {
        // Above node refers to a variable bound by below node.
        return;
    }

    defaultReorder<AboveType, BelowType>(aboveNode, belowNode, ctx);
}

template <class AboveType, class BelowType>
struct RewriteReorder {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        defaultReorderWithDependenceCheck<AboveType, BelowType>(aboveNode, belowNode, ctx);
    }
};

template <>
struct RewriteReorder<ExchangeNode, CollationNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        if (!aboveNode.cast<ExchangeNode>()->getPreserveSort()) {
            // Exchange is above and does not preserve collation. Drop the Collation node.
            ABT newParent = aboveNode;
            newParent.cast<ExchangeNode>()->getChild() =
                belowNode.cast<CollationNode>()->getChild();
            ctx.addNode(newParent);
        }

        defaultReorderWithDependenceCheck<ExchangeNode, CollationNode>(aboveNode, belowNode, ctx);
    }
};

template <class AboveType>
void unwindBelowReorder(ABT::reference_type aboveNode,
                        ABT::reference_type unwindNode,
                        RewriteContext& ctx) {
    const ReorderDependencies dependencies =
        computeDependencies<AboveType, UnwindNode>(aboveNode, unwindNode, ctx);
    if (dependencies._hasNodeRef || dependencies._hasNodeAndChildRef) {
        // Above node refers to projection being unwound. Reject rewrite.
        return;
    }

    defaultReorder<AboveType, UnwindNode>(aboveNode, unwindNode, ctx);
}

template <>
struct RewriteReorder<FilterNode, UnwindNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        unwindBelowReorder<FilterNode>(aboveNode, belowNode, ctx);
    }
};

template <>
struct RewriteReorder<EvaluationNode, UnwindNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        unwindBelowReorder<EvaluationNode>(aboveNode, belowNode, ctx);
    }
};

template <>
struct RewriteReorder<UnwindNode, CollationNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        const ProjectionNameSet& collationProjections =
            belowNode.cast<CollationNode>()->getProperty().getAffectedProjectionNames();
        if (collationProjections.find(aboveNode.cast<UnwindNode>()->getProjectionName()) !=
            collationProjections.cend()) {
            // A projection being affected by the collation is being unwound. Reject rewrite.
            return;
        }

        defaultReorder<UnwindNode, CollationNode>(aboveNode, belowNode, ctx);
    }
};

template <class AboveType, class BelowType>
struct RewriteMerge {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) = delete;
};

template <class T>
static void mergePropertyNodes(ABT::reference_type aboveNodeRef,
                               ABT::reference_type belowNodeRef,
                               RewriteContext& ctx) {
    ABT newRoot = aboveNodeRef;

    T& aboveNode = *newRoot.cast<T>();
    const T& belowNode = *belowNodeRef.cast<T>();

    if (!aboveNode.getProperty().mergeWith(belowNode.getProperty())) {
        // Cannot merge.
        return;
    }

    aboveNode.getChild() = belowNode.getChild();

    // Add merged root.
    ctx.addNode(newRoot);
}

template <>
struct RewriteMerge<CollationNode, CollationNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        mergePropertyNodes<CollationNode>(aboveNode, belowNode, ctx);
    }
};

template <>
struct RewriteMerge<LimitSkipNode, LimitSkipNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        mergePropertyNodes<LimitSkipNode>(aboveNode, belowNode, ctx);
    }
};

template <>
struct RewriteMerge<SargableNode, SargableNode> {
    void operator()(ABT::reference_type aboveNode,
                    ABT::reference_type belowNode,
                    RewriteContext& ctx) const {
        using namespace properties;

        const SargableNode& aboveSargableNode = *aboveNode.cast<SargableNode>();
        const SargableNode& belowSargableNode = *belowNode.cast<SargableNode>();

        PartialSchemaRequirements mergedReqs = aboveSargableNode.getReqMap();
        if (!intersectPartialSchemaReq(mergedReqs, belowSargableNode.getReqMap())) {
            return;
        }

        const std::string& scanDefName =
            *getPropertyConst<CollectionAvailability>(ctx.getAboveLogicalProps())
                 .getScanDefSet()
                 .begin();
        const ScanDefinition& scanDef = ctx.getMetadata()._scanDefs.at(scanDefName);
        auto candidateIndexMap = computeCandidateIndexMap(
            getPropertyConst<IndexingAvailability>(ctx.getAboveLogicalProps()).getScanProjection(),
            mergedReqs,
            scanDef);
        ABT newNode = make<SargableNode>(
            std::move(mergedReqs), std::move(candidateIndexMap), belowSargableNode.getChild());

        ctx.addNode(newNode);
    }
};

template <class Type>
struct RewriteSargableConvert {
    void operator()(ABT::reference_type nodeRef, RewriteContext& ctx) = delete;
};

template <>
struct RewriteSargableConvert<SargableNode> {
    void operator()(ABT::reference_type node, RewriteContext& ctx) {
        const properties::IndexingAvailability& indexingAvailability =
            properties::getPropertyConst<properties::IndexingAvailability>(
                ctx.getAboveLogicalProps());
        const ProjectionName& scanProjectionName = indexingAvailability.getScanProjection();
        const GroupIdType scanGroupId = indexingAvailability.getScanGroupId();

        const SargableNode& sargableNode = *node.cast<SargableNode>();
        if (sargableNode.getCandidateIndexMap().empty()) {
            // Reject rewrite.
            return;
        }

        const ABT& childNode = sargableNode.getChild();
        if (childNode.cast<MemoLogicalDelegatorNode>()->getGroupId() == scanGroupId) {
            // Rewrite not applicable if we have scan group as child.
            return;
        }
        if (collectVariableReferences(node) != VariableNameSetType{scanProjectionName}) {
            // Rewrite not applicable if we refer projections other than the scan projection.
            return;
        }

        ABT leftChild = childNode;
        ABT rightChild = make<SargableNode>(sargableNode.getReqMap(),
                                            sargableNode.getCandidateIndexMap(),
                                            make<MemoLogicalDelegatorNode>(scanGroupId));
        ABT newRoot =
            make<RIDIntersectNode>(scanProjectionName, std::move(leftChild), std::move(rightChild));

        ctx.addNode(newRoot);
    }
};

template <>
struct RewriteSargableConvert<FilterNode> {
    void operator()(ABT::reference_type node, RewriteContext& ctx) {
        using namespace properties;

        const Properties& props = ctx.getAboveLogicalProps();
        if (!hasProperty<IndexingAvailability>(props)) {
            // Can only convert to sargable node if we have indexing availability.
            return;
        }
        const std::string& scanDefName =
            *getPropertyConst<CollectionAvailability>(props).getScanDefSet().begin();
        const ScanDefinition& scanDef = ctx.getMetadata()._scanDefs.at(scanDefName);
        if (scanDef.getIndexDefs().empty()) {
            // Do not extract sargable nodes from filter nodes if we do not have an index on the
            // current scandef.
            return;
        }

        const FilterNode& filterNode = *node.cast<FilterNode>();
        PartialSchemaReqConversion conversion =
            convertExprToPartialSchemaReq(filterNode.getFilter());
        if (!conversion._success) {
            return;
        }

        for (const auto& entry : conversion._reqMap) {
            uassert(0,
                    "Filter partial schema requirement must contain a variable name.",
                    !entry.first._projectionName.empty());
            uassert(0,
                    "Filter partial schema requirement cannot bind.",
                    !entry.second.hasBoundProjectionName());
            uassert(0,
                    "Filter partial schema requirement must have a range.",
                    entry.second.getIntervals() != kIntervalReqFullyOpenDNF);
        }

        auto candidateIndexMap = computeCandidateIndexMap(
            getPropertyConst<IndexingAvailability>(props).getScanProjection(),
            conversion._reqMap,
            scanDef);
        ABT newNode = make<SargableNode>(
            std::move(conversion._reqMap), std::move(candidateIndexMap), filterNode.getChild());

        ctx.addNode(newNode);
    }
};

template <>
struct RewriteSargableConvert<EvaluationNode> {
    void operator()(ABT::reference_type node, RewriteContext& ctx) {
        using namespace properties;

        const Properties& props = ctx.getAboveLogicalProps();
        if (!hasProperty<IndexingAvailability>(props)) {
            // Can only convert to sargable node if we have indexing availability.
            return;
        }

        // We still want to extract sargable nodes from EvalNode to use for PhysicalScans.

        const EvaluationNode& evalNode = *node.cast<EvaluationNode>();
        PartialSchemaReqConversion conversion =
            convertExprToPartialSchemaReq(evalNode.getProjection());
        if (!conversion._success || conversion._reqMap.size() != 1) {
            // For evaluation nodes we expect to create a single entry.
            return;
        }

        for (auto& entry : conversion._reqMap) {
            PartialSchemaRequirement& req = entry.second;
            req.setBoundProjectionName(evalNode.getProjectionName());

            uassert(0,
                    "Eval partial schema requirement must contain a variable name.",
                    !entry.first._projectionName.empty());
            uassert(0,
                    "Eval partial schema requirement cannot have a range",
                    req.getIntervals() == kIntervalReqFullyOpenDNF);
        }

        const std::string& scanDefName =
            *getPropertyConst<CollectionAvailability>(props).getScanDefSet().begin();
        const ScanDefinition& scanDef = ctx.getMetadata()._scanDefs.at(scanDefName);
        auto candidateIndexMap = computeCandidateIndexMap(
            getPropertyConst<IndexingAvailability>(props).getScanProjection(),
            conversion._reqMap,
            scanDef);
        ABT newNode = make<SargableNode>(
            std::move(conversion._reqMap), std::move(candidateIndexMap), evalNode.getChild());

        ctx.addNode(newNode);
    }
};

template <class Type>
struct RewriteConvert {
    void operator()(ABT::reference_type nodeRef, RewriteContext& ctx) = delete;
};

template <>
struct RewriteConvert<GroupByNode> {
    void operator()(ABT::reference_type node, RewriteContext& ctx) {
        const GroupByNode& groupByNode = *node.cast<GroupByNode>();
        if (!groupByNode.canRewriteIntoLocal()) {
            return;
        }

        ProjectionNameVector preaggVariableNames;
        ABTVector preaggExpressions;

        const ABTVector& aggExpressions = groupByNode.getAggregationExpressions();
        for (const ABT& expr : aggExpressions) {
            const FunctionCall* aggPtr = expr.cast<FunctionCall>();
            if (aggPtr == nullptr) {
                return;
            }

            // In order to be able to pre-aggregate for now we expect a simple aggregate like
            // SUM(x).
            const auto& aggFnName = aggPtr->name();
            if (aggFnName != "$sum" && aggFnName != "$min" && aggFnName != "$max") {
                // TODO: allow more functions.
                return;
            }
            uassert(0, "Invalid argument count", aggPtr->nodes().size() == 1);

            preaggVariableNames.push_back(ctx.getPrefixId().getNextId("preagg"));
            preaggExpressions.emplace_back(make<FunctionCall>(
                aggFnName, ABTVector{make<Variable>(preaggVariableNames.back())}));
        }

        ABT localGroupBy = make<GroupByNode>(groupByNode.getGroupByProjectionNames(),
                                             std::move(preaggVariableNames),
                                             aggExpressions,
                                             true /*isLocal*/,
                                             groupByNode.getChild());

        ABT newRoot = make<GroupByNode>(groupByNode.getGroupByProjectionNames(),
                                        groupByNode.getAggregationProjectionNames(),
                                        std::move(preaggExpressions),
                                        false /*isLocal*/,
                                        std::move(localGroupBy));

        ctx.addNode(newRoot);
    }
};

LogicalRewriter::RewriteFnMap LogicalRewriter::initializeRewrites() {
    RewriteFnMap result;
    const auto setRewriteEntry = [this, &result](const LogicalRewriteType rewriteType,
                                                 RewriteFn fn) {
        if (_activeRewriteSet.find(rewriteType) != _activeRewriteSet.cend()) {
            result.emplace(rewriteType, fn);
        }
    };

    setRewriteEntry(LogicalRewriteType::FilterFilterReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, FilterNode, RewriteReorder>);
    setRewriteEntry(
        LogicalRewriteType::EvaluationEvaluationReorder,
        &LogicalRewriter::bindAboveBelow<EvaluationNode, EvaluationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::FilterEvaluationReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, EvaluationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::EvaluationFilterReorder,
                    &LogicalRewriter::bindAboveBelow<EvaluationNode, FilterNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::FilterCollationReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, CollationNode, RewriteReorder>);
    setRewriteEntry(
        LogicalRewriteType::EvaluationCollationReorder,
        &LogicalRewriter::bindAboveBelow<EvaluationNode, CollationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::FilterLimitSkipReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, LimitSkipNode, RewriteReorder>);
    setRewriteEntry(
        LogicalRewriteType::EvaluationLimitSkipReorder,
        &LogicalRewriter::bindAboveBelow<EvaluationNode, LimitSkipNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::FilterGroupByReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, GroupByNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::GroupCollationReorder,
                    &LogicalRewriter::bindAboveBelow<GroupByNode, CollationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::FilterUnwindReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, UnwindNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::EvaluationUnwindReorder,
                    &LogicalRewriter::bindAboveBelow<EvaluationNode, UnwindNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::UnwindCollationReorder,
                    &LogicalRewriter::bindAboveBelow<UnwindNode, CollationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::UnwindLimitSkipReorder,
                    &LogicalRewriter::bindAboveBelow<UnwindNode, LimitSkipNode, RewriteReorder>);

    setRewriteEntry(LogicalRewriteType::FilterExchangeReorder,
                    &LogicalRewriter::bindAboveBelow<FilterNode, ExchangeNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::ExchangeEvaluationReorder,
                    &LogicalRewriter::bindAboveBelow<ExchangeNode, EvaluationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::ExchangeCollationReorder,
                    &LogicalRewriter::bindAboveBelow<ExchangeNode, CollationNode, RewriteReorder>);
    setRewriteEntry(LogicalRewriteType::ExchangeLimitSkipReorder,
                    &LogicalRewriter::bindAboveBelow<ExchangeNode, LimitSkipNode, RewriteReorder>);

    setRewriteEntry(LogicalRewriteType::CollationMerge,
                    &LogicalRewriter::bindAboveBelow<CollationNode, CollationNode, RewriteMerge>);
    setRewriteEntry(LogicalRewriteType::LimitSkipMerge,
                    &LogicalRewriter::bindAboveBelow<LimitSkipNode, LimitSkipNode, RewriteMerge>);

    setRewriteEntry(LogicalRewriteType::GroupByLocalGlobal,
                    &LogicalRewriter::bindSingleNode<GroupByNode, RewriteConvert>);

    setRewriteEntry(LogicalRewriteType::SargableMerge,
                    &LogicalRewriter::bindAboveBelow<SargableNode, SargableNode, RewriteMerge>);
    setRewriteEntry(LogicalRewriteType::SargableFilterConvert,
                    &LogicalRewriter::bindSingleNode<FilterNode, RewriteSargableConvert>);
    setRewriteEntry(LogicalRewriteType::SargableEvaluationConvert,
                    &LogicalRewriter::bindSingleNode<EvaluationNode, RewriteSargableConvert>);
    setRewriteEntry(LogicalRewriteType::SargableSplit,
                    &LogicalRewriter::bindSingleNode<SargableNode, RewriteSargableConvert>);

    return result;
}

bool LogicalRewriter::rewriteToFixPoint() {
    const RewriteFnMap rewriteMap = initializeRewrites();

    int iterationCount = 0;

    while (!_queue.empty()) {
        iterationCount++;
        if (_memo.getDebugInfo().exceedsIterationLimit(iterationCount)) {
            // Iteration limit exceeded.
            return false;
        }

        RewriteEntry rewriteEntry = std::move(_queue.top());
        _queue.pop();

        rewriteMap.at(rewriteEntry._type)(this, rewriteEntry._nodeId);
    }

    return true;
}

template <class AboveType, class BelowType, template <class, class> class R>
void LogicalRewriter::bindAboveBelow(const Memo::NodeId nodeMemoId) {
    // Get a reference to the node instead of the node itself.
    // Rewrites insert into the memo and can move it.
    ABT::reference_type node = _memo.getNode(nodeMemoId);
    const GroupIdType currentGroupId = nodeMemoId.first;

    if (node.is<AboveType>()) {
        // Try to bind as parent.
        const GroupIdType targetGroupId = node.cast<AboveType>()
                                              ->getChild()
                                              .template cast<MemoLogicalDelegatorNode>()
                                              ->getGroupId();

        RewriteContext ctx(*this, currentGroupId);
        for (size_t i = 0; i < _memo.getGroup(targetGroupId)._logicalNodes.size(); i++) {
            auto targetNode = _memo.getNode({targetGroupId, i});
            if (targetNode.is<BelowType>()) {
                R<AboveType, BelowType>()(node, targetNode, ctx);
            }
        }
    }

    if (node.is<BelowType>()) {
        // Try to bind as child.
        Memo::NodeIdSet usageNodeIdSet;
        {
            const auto& inputGroupsToNodeId = _memo.getInputGroupsToNodeIdMap();
            auto it = inputGroupsToNodeId.find({currentGroupId});
            if (it != inputGroupsToNodeId.cend()) {
                usageNodeIdSet = it->second;
            }
        }

        for (const Memo::NodeId& parentNodeId : usageNodeIdSet) {
            auto targetNode = _memo.getNode(parentNodeId);
            if (targetNode.is<AboveType>()) {
                uassert(6624000,
                        "Parent child groupId mismatch (usage map index incorrect?)",
                        targetNode.cast<AboveType>()
                                ->getChild()
                                .template cast<MemoLogicalDelegatorNode>()
                                ->getGroupId() == currentGroupId);

                RewriteContext ctx(*this, parentNodeId.first);
                R<AboveType, BelowType>()(targetNode, node, ctx);
            }
        }
    }
}

template <class Type, template <class> class R>
void LogicalRewriter::bindSingleNode(const Memo::NodeId nodeMemoId) {
    // Get a reference to the node instead of the node itself.
    // Rewrites insert into the memo and can move it.
    ABT::reference_type node = _memo.getNode(nodeMemoId);

    if (node.is<Type>()) {
        RewriteContext ctx(*this, nodeMemoId.first);
        R<Type>()(node, ctx);
    }
}

class MemoLatestPlanExtractor {
public:
    explicit MemoLatestPlanExtractor(Memo& memo) : _memo(memo) {}

    /**
     * Logical delegator node.
     */
    void transport(ABT& n,
                   const MemoLogicalDelegatorNode& node,
                   std::unordered_set<GroupIdType>& visitedGroups) {
        n = extractLatest(node.getGroupId(), visitedGroups);
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    void transport(ABT& /*n*/,
                   const T& /*node*/,
                   std::unordered_set<GroupIdType>& visitedGroups,
                   Ts&&...) {
        // noop
    }

    ABT extractLatest(const GroupIdType groupId, std::unordered_set<GroupIdType>& visitedGroups) {
        const Group& group = _memo.getGroup(groupId);
        if (!visitedGroups.insert(groupId).second) {
            const GroupIdType scanGroupId =
                properties::getPropertyConst<properties::IndexingAvailability>(
                    group._logicalProperties)
                    .getScanGroupId();
            uassert(
                6624000, "Visited the same non-scan group more than once", groupId == scanGroupId);
        }

        ABT rootNode = group._logicalNodes.getVector().back();
        algebra::transport<true>(rootNode, *this, visitedGroups);
        return rootNode;
    }

private:
    Memo& _memo;
};

ABT LogicalRewriter::getLatestPlan(const GroupIdType rootGroupId) {
    MemoLatestPlanExtractor extractor(_memo);
    std::unordered_set<GroupIdType> visitedGroups;
    return extractor.extractLatest(rootGroupId, visitedGroups);
}

const LogicalRewriter::RewriteSet& LogicalRewriter::getRewriteSet() {
    return _rewriteSet;
}

LogicalRewriter::QueueType& LogicalRewriter::getQueue() {
    return _queue;
}

}  // namespace mongo::optimizer::cascades
