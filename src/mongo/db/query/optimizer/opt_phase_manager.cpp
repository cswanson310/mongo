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

#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/db/query/optimizer/cascades/ce_heuristic.h"
#include "mongo/db/query/optimizer/cascades/logical_props_derivation.h"
#include "mongo/db/query/optimizer/cascades/physical_rewriter.h"
#include "mongo/db/query/optimizer/rewrites/const_eval.h"
#include "mongo/db/query/optimizer/rewrites/filter.h"
#include "mongo/db/query/optimizer/rewrites/index_bounds_lower.h"
#include "mongo/db/query/optimizer/rewrites/path.h"
#include "mongo/db/query/optimizer/rewrites/path_lower.h"

namespace mongo::optimizer {

OptPhaseManager::PhaseSet OptPhaseManager::_allRewrites = {OptPhase::ConstEvalPre,
                                                           OptPhase::PathFuse,
                                                           OptPhase::Filter,
                                                           OptPhase::MemoLogicalRewritePhase,
                                                           OptPhase::MemoPhysicalRewritePhase,
                                                           OptPhase::IndexBoundsLower,
                                                           OptPhase::PathLower,
                                                           OptPhase::ConstEvalPost};

OptPhaseManager::OptPhaseManager(OptPhaseManager::PhaseSet phaseSet,
                                 PrefixId& prefixId,
                                 Metadata metadata,
                                 DebugInfo debugInfo)
    : OptPhaseManager(std::move(phaseSet),
                      prefixId,
                      false /*requireRID*/,
                      std::move(metadata),
                      heuristicCE,
                      std::move(debugInfo)) {}

OptPhaseManager::OptPhaseManager(OptPhaseManager::PhaseSet phaseSet,
                                 PrefixId& prefixId,
                                 const bool requireRID,
                                 Metadata metadata,
                                 CEDeriveFn ceDeriveFn,
                                 DebugInfo debugInfo)
    : _phaseSet(std::move(phaseSet)),
      _debugInfo(std::move(debugInfo)),
      _metadata(std::move(metadata)),
      _memo(_debugInfo,
            std::bind(logicalPropsDerive,
                      std::placeholders::_1,
                      std::cref(_metadata),
                      std::placeholders::_2),
            ceDeriveFn),
      _physicalNodeId(),
      _requireRID(requireRID),
      _physicalPlansExplorationCount(0),
      _prefixId(prefixId) {}

template <class C>
bool OptPhaseManager::runStructuralPhase(C instance,
                                         const OptPhase phase,
                                         VariableEnvironment& env,
                                         ABT& input) {
    if (!hasPhase(phase)) {
        return true;
    }

    for (int iterationCount = 0; instance.optimize(input); iterationCount++) {
        if (_debugInfo.exceedsIterationLimit(iterationCount)) {
            // Iteration limit exceeded.
            return false;
        }
    }

    return !env.hasFreeVariables();
}

bool OptPhaseManager::runMemoLogicalRewrite(const OptPhase phase,
                                            VariableEnvironment& env,
                                            const LogicalRewriter::RewriteSet& rewriteSet,
                                            GroupIdType& rootGroupId,
                                            ABT& input) {
    if (!hasPhase(phase)) {
        return true;
    }

    LogicalRewriter rewriter(_memo, _prefixId, _metadata, rewriteSet);

    if (rootGroupId < 0) {
        rootGroupId = rewriter.addInitialNode(input);
    } else {
        // We have an existing memo. Add all nodes to queue.
        // TODO: optimize, insert only relevant node types for rewrites in the phase.
        LogicalRewriter::QueueType& queue = rewriter.getQueue();

        for (size_t groupId = 0; groupId < _memo.getGroupCount(); groupId++) {
            const size_t nodeCount = _memo.getGroup(groupId)._logicalNodes.size();
            for (size_t nodeId = 0; nodeId < nodeCount; nodeId++) {
                for (const auto& entry : rewriteSet) {
                    queue.push({entry.first, entry.second, {groupId, nodeId}});
                }
            }
        }
    }

    if (!rewriter.rewriteToFixPoint()) {
        return false;
    }

    input = rewriter.getLatestPlan(rootGroupId);
    env.rebuild(input);

    return true;
}

bool OptPhaseManager::runMemoPhysicalRewrite(const OptPhase phase,
                                             VariableEnvironment& env,
                                             const GroupIdType rootGroupId,
                                             ABT& input) {
    if (!hasPhase(phase)) {
        return true;
    }
    if (rootGroupId < 0) {
        // Logical rewrites did not run?
        return false;
    }

    // By default we require centralized result.
    // Also by default we do not require projections: the Root node will add those.
    properties::Properties props = properties::makeProperties(
        properties::DistributionRequirement(DistributionType::Centralized));
    if (_requireRID) {
        properties::setProperty<properties::IndexingRequirement>(
            props,
            properties::IndexingRequirement(properties::IndexReqTarget::Complete,
                                            _prefixId.getNextId("rid")));
    }

    PhysicalRewriter rewriter(_memo, _metadata);

    auto optGroupResult =
        rewriter.optimizeGroup(rootGroupId, std::move(props), _prefixId, CostType::kInfinity);
    if (!optGroupResult._success) {
        return false;
    }

    _physicalPlansExplorationCount = rewriter.getPlanExplorationCount();
    _physicalNodeId = {rootGroupId, optGroupResult._index};

    auto [node, nodeMap] = extractPhysicalPlan(_physicalNodeId, _memo, _prefixId);
    input = std::move(node);
    _nodeToPhysPropsMap = std::move(nodeMap);

    env.rebuild(input);

    return true;
}

bool OptPhaseManager::runMemoRewritePhases(VariableEnvironment& env, ABT& input) {
    _memo.clear();
    GroupIdType rootGroupId = -1;

    if (!runMemoLogicalRewrite(OptPhase::MemoLogicalRewritePhase,
                               env,
                               LogicalRewriter::getRewriteSet(),
                               rootGroupId,
                               input)) {
        return false;
    }

    if (!runMemoPhysicalRewrite(OptPhase::MemoPhysicalRewritePhase, env, rootGroupId, input)) {
        return false;
    }

    return true;
}

bool OptPhaseManager::optimize(ABT& input) {
    VariableEnvironment env = VariableEnvironment::build(input);
    if (env.hasFreeVariables()) {
        return false;
    }

    if (!runStructuralPhase(ConstEval{env}, OptPhase::ConstEvalPre, env, input)) {
        return false;
    }
    if (!runStructuralPhase(PathFusion{env}, OptPhase::PathFuse, env, input)) {
        return false;
    }
    if (!runStructuralPhase(FilterRewriter{_prefixId}, OptPhase::Filter, env, input)) {
        return false;
    }

    if (!runMemoRewritePhases(env, input)) {
        return false;
    }

    if (!runStructuralPhase(
            IndexBoundsLowerRewriter{_prefixId}, OptPhase::IndexBoundsLower, env, input)) {
        return false;
    }
    if (!runStructuralPhase(PathLowering{env}, OptPhase::PathLower, env, input)) {
        return false;
    }
    if (!runStructuralPhase(ConstEval{env}, OptPhase::ConstEvalPost, env, input)) {
        return false;
    }

    env.rebuild(input);
    if (env.hasFreeVariables()) {
        return false;
    }

    return true;
}

bool OptPhaseManager::hasPhase(const OptPhase phase) const {
    return _phaseSet.find(phase) != _phaseSet.cend();
}

const OptPhaseManager::PhaseSet& OptPhaseManager::getAllRewritesSet() {
    return _allRewrites;
}

MemoPhysicalNodeId OptPhaseManager::getPhysicalNodeId() const {
    return _physicalNodeId;
}

size_t OptPhaseManager::getPhysicalPlanExplorationCount() const {
    return _physicalPlansExplorationCount;
}

const Memo& OptPhaseManager::getMemo() const {
    return _memo;
}

const Metadata& OptPhaseManager::getMetadata() const {
    return _metadata;
}

PrefixId& OptPhaseManager::getPrefixId() const {
    return _prefixId;
}

const std::unordered_map<const Node*, MemoPhysicalNodeId>& OptPhaseManager::getNodeToPhysPropsMap()
    const {
    return _nodeToPhysPropsMap;
}

}  // namespace mongo::optimizer
