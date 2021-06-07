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

#include <unordered_set>

#include "mongo/db/query/optimizer/cascades/logical_rewriter.h"

namespace mongo::optimizer {

using namespace cascades;

/**
 * This class wraps together different optimization phases.
 * First the transport rewrites are applied such as constant folding and redundant expression
 * elimination. Second the logical and physical reordering rewrites are applied using the memo.
 * Third the final transport rewritesd are applied.
 */
class OptPhaseManager {
public:
    enum class OptPhase {
        //  ConstEval performs the following rewrites: constant folding, inlining, and dead code
        //  elimination.
        ConstEvalPre,
        PathFuse,

        // Filter rewrites manipulate filter expressions.
        Filter,

        // Most logical rewrites are in this phase, such as filter-filter reorder, evaluation-filter
        // reorder and similar.
        MemoLogicalRewritePhase,
        // Cascades-style optimization.
        MemoPhysicalRewritePhase,

        // Lowers compound index intervals to singular ones.
        IndexBoundsLower,

        PathLower,
        ConstEvalPost
    };

    using PhaseSet = std::unordered_set<OptPhase>;

    OptPhaseManager(PhaseSet phaseSet, PrefixId& prefixId, Metadata metadata, DebugInfo debugInfo);
    OptPhaseManager(PhaseSet phaseSet,
                    PrefixId& prefixId,
                    const bool requireRID,
                    Metadata metadata,
                    CEDeriveFn ceDeriveFn,
                    DebugInfo debugInfo);

    /**
     * Optimization modifies the input argument.
     * Return result is true for successful optimization and false for failure.
     */
    bool optimize(ABT& input);

    static const PhaseSet& getAllRewritesSet();

    MemoPhysicalNodeId getPhysicalNodeId() const;

    size_t getPhysicalPlanExplorationCount() const;

    const Memo& getMemo() const;

    const Metadata& getMetadata() const;

    PrefixId& getPrefixId() const;

    const std::unordered_map<const Node*, MemoPhysicalNodeId>& getNodeToPhysPropsMap() const;
    std::unordered_map<const Node*, MemoPhysicalNodeId>& getNodeToPhysPropsMap();

private:
    bool hasPhase(const OptPhase phase) const;

    template <class C>
    bool runStructuralPhase(C instance, const OptPhase phase, VariableEnvironment& env, ABT& input);

    bool runMemoLogicalRewrite(const OptPhase phase,
                               VariableEnvironment& env,
                               const LogicalRewriter::RewriteSet& rewriteSet,
                               GroupIdType& rootGroupId,
                               ABT& input);

    bool runMemoPhysicalRewrite(const OptPhase phase,
                                VariableEnvironment& env,
                                const GroupIdType rootGroupId,
                                ABT& input);

    bool runMemoRewritePhases(VariableEnvironment& env, ABT& input);


    static PhaseSet _allRewrites;

    const PhaseSet _phaseSet;

    const DebugInfo _debugInfo;

    Metadata _metadata;

    /**
     * Final state of the memo after physical rewrites are complete.
     */
    Memo _memo;

    /**
     * Root physical node if we have performed physical rewrites.
     */
    MemoPhysicalNodeId _physicalNodeId;

    /**
     * Map from node to physical properties. Used to determine for example which of the available
     * projections are used for exchanges.
     */
    std::unordered_map<const Node*, MemoPhysicalNodeId> _nodeToPhysPropsMap;

    /**
     * Used to optimize update and delete statements. If set will include indexing requirement with
     * seed physical properties.
     */
    const bool _requireRID;

    /**
     * Number of explored plans if we performed physical rewrites.
     */
    size_t _physicalPlansExplorationCount;

    // We don't own this.
    PrefixId& _prefixId;
};

}  // namespace mongo::optimizer
