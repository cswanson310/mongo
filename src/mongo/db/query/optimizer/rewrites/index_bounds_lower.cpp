/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#include "mongo/db/query/optimizer/rewrites/index_bounds_lower.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer {

ABT IndexBoundsLowerRewriter::createIntersectOrUnionForIndexLower(
    const bool isIntersect,
    ABTVector inputs,
    const FieldProjectionMap& innerMap,
    const FieldProjectionMap& outerMap) {
    const size_t inputSize = inputs.size();
    if (inputSize == 1) {
        return std::move(inputs.front());
    }

    ProjectionNameVector unionProjectionNames;
    unionProjectionNames.push_back(innerMap._ridProjection);
    for (const auto& [fieldName, projectionName] : innerMap._fieldProjections) {
        unionProjectionNames.push_back(projectionName);
    }

    ProjectionNameVector aggProjectionNames;
    for (const auto& [fieldName, projectionName] : outerMap._fieldProjections) {
        aggProjectionNames.push_back(projectionName);
    }

    ProjectionName countProjection;
    if (isIntersect) {
        // Last agg projection is for counting.
        countProjection = _prefixId.getNextId("count");
        aggProjectionNames.push_back(countProjection);
    }

    ABTVector aggExpressions;
    for (const auto& [fieldName, projectionName] : innerMap._fieldProjections) {
        aggExpressions.emplace_back(
            make<FunctionCall>("$first", ABTVector{make<Variable>(projectionName)}));
    }
    if (isIntersect) {
        aggExpressions.emplace_back(make<FunctionCall>("$sum", ABTVector{Constant::int64(1)}));
    }

    ABT result = make<UnionNode>(std::move(unionProjectionNames), std::move(inputs));
    result = make<GroupByNode>(ProjectionNameVector{innerMap._ridProjection},
                               std::move(aggProjectionNames),
                               std::move(aggExpressions),
                               std::move(result));

    if (isIntersect) {
        result = make<FilterNode>(
            make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(inputSize)),
                             make<Variable>(countProjection)),
            std::move(result));
    }
    return result;
}

void IndexBoundsLowerRewriter::transport(ABT& n, const IndexScanNode& node, ABT&) {
    const auto& spec = node.getIndexSpecification();
    const auto& intervals = spec.getIntervals();
    const bool isSingularDisjunction = intervals.size() == 1;
    if (isSingularDisjunction && intervals.front().size() == 1) {
        // Already lowered.
        return;
    }

    FieldProjectionMap outerMap = node.getFieldProjectionMap();
    if (outerMap._ridProjection.empty()) {
        outerMap._ridProjection = _prefixId.getNextId("rid");
    }
    if (!isSingularDisjunction) {
        for (auto& [fieldName, projectionName] : outerMap._fieldProjections) {
            projectionName = _prefixId.getNextId("outer");
        }
    }

    ABTVector disjuncts;
    for (const auto& conjunction : intervals) {
        const bool isSingularConjunction = conjunction.size() == 1;
        FieldProjectionMap innerMap = outerMap;
        if (!isSingularConjunction) {
            for (auto& [fieldName, projectionName] : innerMap._fieldProjections) {
                projectionName = _prefixId.getNextId("inner");
            }
        }

        ABTVector conjuncts;
        for (const auto& interval : conjunction) {
            ABT physicalIndexScan = make<IndexScanNode>(innerMap,
                                                        IndexSpecification{spec.getScanDefName(),
                                                                           spec.getIndexDefName(),
                                                                           {{interval}},
                                                                           spec.isReverseOrder()});
            conjuncts.emplace_back(std::move(physicalIndexScan));
        }

        disjuncts.emplace_back(createIntersectOrUnionForIndexLower(
            true /*isIntersect*/, std::move(conjuncts), innerMap, outerMap));
    }

    // We are updating the node, re-insert it into the phys props map of the manager.
    auto& nodeToPhysPropsMap = _phaseManager.getNodeToPhysPropsMap();
    const auto nodeId = nodeToPhysPropsMap.at(n.cast<Node>());
    n = createIntersectOrUnionForIndexLower(
        false /*isIntersect*/, std::move(disjuncts), outerMap, node.getFieldProjectionMap());
    nodeToPhysPropsMap.emplace(n.cast<Node>(), nodeId);
}

bool IndexBoundsLowerRewriter::optimize(ABT& n) {
    algebra::transport<true>(n, *this);
    return false;
}
}  // namespace mongo::optimizer
