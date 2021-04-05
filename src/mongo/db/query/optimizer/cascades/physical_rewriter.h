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

#include "mongo/db/query/optimizer/cascades/memo.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

class PhysicalRewriter {
    friend class PropEnforcerVisitor;
    friend class ImplementationVisitor;

public:
    struct OptimizeGroupResult {
        OptimizeGroupResult();
        OptimizeGroupResult(const size_t index, const CostType cost);

        OptimizeGroupResult(const OptimizeGroupResult& other) = default;
        OptimizeGroupResult(OptimizeGroupResult&& other) = default;

        bool _success;
        size_t _index;
        CostType _cost;
    };

    using CostEstimationFn = std::function<CostType(const Metadata& metadata,
                                                    Memo& memo,
                                                    const properties::Properties& logicalProps,
                                                    const properties::Properties& physProps,
                                                    const ABT::reference_type physNodeRef)>;

    PhysicalRewriter(Memo& memo,
                     ProjectionName leftRIDProjectionName,
                     ProjectionName rightRIDProjectionName,
                     const Metadata& metadata);
    PhysicalRewriter(Memo& memo,
                     ProjectionName leftRIDProjectionName,
                     ProjectionName rightRIDProjectionName,
                     const Metadata& metadata,
                     const CostEstimationFn& costFn);

    /**
     * Main entry point for physical optimization.
     * Optimize a logical plan rooted at a RootNode, and return an index into the winner's circle if
     * successful.
     */
    OptimizeGroupResult optimizeGroup(const GroupIdType groupId,
                                      const properties::Properties& physProps,
                                      PrefixId prefixId,
                                      const CostType costLimit);

    size_t getPlanExplorationCount() const;

private:
    void costAndRetainBestNode(ABT node,
                               ChildPropsType childProps,
                               const GroupIdType groupId,
                               const properties::Properties& logicalProps,
                               const PrefixId& prefixId,
                               PhysOptimizationResult& bestResult);

    std::pair<bool, CostType> optimizeChildren(const CostType nodeCost,
                                               ChildPropsType childProps,
                                               const PrefixId& prefixId,
                                               const CostType costLimit);

    PhysicalRewriter::OptimizeGroupResult applyRewrites(const GroupIdType groupId,
                                                        const size_t localPlanExplorationCount,
                                                        const Group& group,
                                                        PrefixId& prefixId,
                                                        PhysOptimizationResult& bestResult);

    // Number of recursive optimization calls.
    size_t _planExplorationCount;

    // Used to intersect rowIds.
    ProjectionName _leftRIDProjectionName;
    ProjectionName _rightRIDProjectionName;

    // We don't own any of this.
    Memo& _memo;
    const CostEstimationFn _costFn;
    const Metadata& _metadata;
};

}  // namespace mongo::optimizer::cascades
