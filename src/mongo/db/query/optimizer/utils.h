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
#include "mongo/db/query/optimizer/defs.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/props.h"

namespace mongo::optimizer {

inline void updateHash(size_t& result, const size_t hash) {
    result = 31 * result + hash;
}

template <class T>
inline size_t computeVectorHash(const std::vector<T>& v) {
    size_t result = 17;
    for (const T e : v) {
        updateHash(result, std::hash<T>()(e));
    }
    return result;
}

template <int typeCode, typename... Args>
inline size_t computeHashSeq(const Args&... seq) {
    size_t result = 17 + typeCode;
    (updateHash(result, seq), ...);
    return result;
}

/**
 * Used to vend out fresh ids for projection names.
 */
class PrefixId {
public:
    std::string getNextId(const std::string& key);

private:
    std::unordered_map<std::string, int> _idCounterPerKey;
};

ProjectionNameOrderedSet convertToOrderedSet(ProjectionNameSet unordered);

/**
 * Used to track references originating from a set of properties.
 */
ProjectionNameSet extractReferencedColumns(const properties::Properties& properties);

bool combineMultiKeyIntervals(MultiKeyIntervalRequirementDNF& intervals,
                              const IntervalRequirementDNF& sourceIntervals);

/**
 * Used to extract variable references from a node.
 */
using VariableNameSetType = std::unordered_set<std::string>;
VariableNameSetType collectVariableReferences(const ABT& n);

struct PartialSchemaReqConversion {
    PartialSchemaReqConversion();
    PartialSchemaReqConversion(PartialSchemaRequirements reqMap);
    PartialSchemaReqConversion(ABT bound);

    bool hasBound() const;

    bool _success;
    ABT _bound;

    PartialSchemaRequirements _reqMap;
};

PartialSchemaReqConversion convertExprToPartialSchemaReq(const ABT& expr);

bool intersectPartialSchemaReq(PartialSchemaRequirements& target,
                               const PartialSchemaRequirements& source);

std::string computeIndexKeyName(const size_t indexField);

CandidateIndexMap computeCandidateIndexMap(const ProjectionName& scanProjectionName,
                                           const PartialSchemaRequirements& reqMap,
                                           const ScanDefinition& scanDef);

/**
 * Extracts a complete physical plan by inlining references to MemoPhysicalPlanNode.
 */
std::pair<ABT, std::unordered_map<const Node*, MemoPhysicalNodeId>> extractPhysicalPlan(
    const MemoPhysicalNodeId id, const cascades::Memo& memo, PrefixId& prefixId);

}  // namespace mongo::optimizer
