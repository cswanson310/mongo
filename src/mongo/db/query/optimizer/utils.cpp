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

#include "mongo/db/query/optimizer/utils.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/reference_tracker.h"

namespace mongo::optimizer {

std::string PrefixId::getNextId(const std::string& key) {
    std::ostringstream os;
    os << key << "_" << _idCounterPerKey[key]++;
    return os.str();
}

ProjectionNameOrderedSet convertToOrderedSet(ProjectionNameSet unordered) {
    ProjectionNameOrderedSet ordered;
    for (const ProjectionName& projection : unordered) {
        ordered.emplace(projection);
    }
    return ordered;
}

/**
 * Used to track references originating from a set of properties.
 */
class PropertiesAffectedColumnsExtractor {
public:
    template <class T>
    void operator()(const properties::Property&, const T& prop) {
        if constexpr (std::is_base_of_v<properties::PhysicalProperty, T>) {
            for (const ProjectionName& projection : prop.getAffectedProjectionNames()) {
                _projections.insert(projection);
            }
        }
    }

    static ProjectionNameSet extract(const properties::Properties& properties) {
        PropertiesAffectedColumnsExtractor extractor;
        for (const auto& entry : properties) {
            entry.second.visit(extractor);
        }
        return extractor._projections;
    }

private:
    ProjectionNameSet _projections;
};

ProjectionNameSet extractReferencedColumns(const properties::Properties& properties) {
    return PropertiesAffectedColumnsExtractor::extract(properties);
}

/**
 * Helper class used to extract variable references from a node.
 */
class NodeVariableTracker {
public:
    template <typename T, typename... Ts>
    VariableNameSetType walk(const T&, Ts&&...) {
        static_assert(!std::is_base_of_v<Node, T>, "Nodes must implement variable tracking");

        // Default case: no variables.
        return {};
    }

    VariableNameSetType walk(const ScanNode& /*node*/, const ABT& /*binds*/) {
        return {};
    }

    VariableNameSetType walk(const PhysicalScanNode& /*node*/, const ABT& /*binds*/) {
        return {};
    }

    VariableNameSetType walk(const CoScanNode& /*node*/) {
        return {};
    }

    VariableNameSetType walk(const IndexScanNode& /*node*/, const ABT& /*binds*/) {
        return {};
    }

    VariableNameSetType walk(const SeekNode& /*node*/, const ABT& /*binds*/, const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const MemoLogicalDelegatorNode& /*node*/) {
        return {};
    }

    VariableNameSetType walk(const MemoPhysicalDelegatorNode& /*node*/) {
        return {};
    }

    VariableNameSetType walk(const FilterNode& /*node*/, const ABT& /*child*/, const ABT& expr) {
        return extractFromABT(expr);
    }

    VariableNameSetType walk(const EvaluationNode& /*node*/,
                             const ABT& /*child*/,
                             const ABT& expr) {
        return extractFromABT(expr);
    }

    VariableNameSetType walk(const SargableNode& /*node*/,
                             const ABT& /*child*/,
                             const ABT& /*binds*/,
                             const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const RIDIntersectNode& /*node*/,
                             const ABT& /*leftChild*/,
                             const ABT& /*rightChild*/) {
        return {};
    }

    VariableNameSetType walk(const BinaryJoinNode& /*node*/,
                             const ABT& /*leftChild*/,
                             const ABT& /*rightChild*/,
                             const ABT& expr) {
        return extractFromABT(expr);
    }

    VariableNameSetType walk(const HashJoinNode& /*node*/,
                             const ABT& /*leftChild*/,
                             const ABT& /*rightChild*/,
                             const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const InnerMultiJoinNode& /*node*/, const ABTVector& /*children*/) {
        return {};
    }

    VariableNameSetType walk(const UnionNode& /*node*/,
                             const ABTVector& /*children*/,
                             const ABT& /*binder*/,
                             const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const GroupByNode& /*node*/,
                             const ABT& /*child*/,
                             const ABT& /*aggBinder*/,
                             const ABT& aggRefs,
                             const ABT& /*groupbyBinder*/,
                             const ABT& groupbyRefs) {
        VariableNameSetType result;
        extractFromABT(result, aggRefs);
        extractFromABT(result, groupbyRefs);
        return result;
    }

    VariableNameSetType walk(const UnwindNode& /*node*/,
                             const ABT& /*child*/,
                             const ABT& /*binds*/,
                             const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const WindNode& /*node*/,
                             const ABT& /*child*/,
                             const ABT& /*binds*/,
                             const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const CollationNode& /*node*/, const ABT& /*child*/, const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const LimitSkipNode& /*node*/, const ABT& /*child*/) {
        return {};
    }

    VariableNameSetType walk(const ExchangeNode& /*node*/, const ABT& /*child*/, const ABT& refs) {
        return extractFromABT(refs);
    }

    VariableNameSetType walk(const RootNode& /*node*/, const ABT& /*child*/, const ABT& refs) {
        return extractFromABT(refs);
    }

    static VariableNameSetType collect(const ABT& n) {
        NodeVariableTracker tracker;
        return algebra::walk<false>(n, tracker);
    }

private:
    void extractFromABT(VariableNameSetType& vars, const ABT& v) {
        const auto& result = VariableEnvironment::getVariables(v);
        for (const Variable* var : result._variables) {
            if (result._definedVars.count(var->name()) == 0) {
                // We are interested in either free variables, or variables defined on other nodes.
                vars.insert(var->name());
            }
        }
    }

    VariableNameSetType extractFromABT(const ABT& v) {
        VariableNameSetType result;
        extractFromABT(result, v);
        return result;
    }
};

VariableNameSetType collectVariableReferences(const ABT& n) {
    return NodeVariableTracker::collect(n);
}

PartialSchemaReqConversion::PartialSchemaReqConversion()
    : _success(false), _bound(make<Blackhole>()), _reqMap() {}

PartialSchemaReqConversion::PartialSchemaReqConversion(PartialSchemaRequirements reqMap)
    : _success(true), _bound(make<Blackhole>()), _reqMap(std::move(reqMap)) {}

PartialSchemaReqConversion::PartialSchemaReqConversion(ABT bound)
    : _success(true), _bound(std::move(bound)), _reqMap() {}

bool PartialSchemaReqConversion::hasBound() const {
    return _bound != make<Blackhole>();
}

static void combineIntervals(const bool intersect,
                             IntervalRequirementDNF& target,
                             const IntervalRequirementDNF& source) {
    if (target == source) {
        // Intervals are the same. Leave target unchanged.
        return;
    }

    if (target == kIntervalReqFullyOpenDNF) {
        // Intersecting with fully open interval is redundant.
        // Unioning with fully open interval results in a fully-open interval.
        if (intersect) {
            target = source;
        }
        return;
    }

    if (source == kIntervalReqFullyOpenDNF) {
        // Intersecting with fully open interval is redundant.
        // Unioning with fully open interval results in a fully-open interval.
        if (!intersect) {
            target = source;
        }
        return;
    }

    // Integrate both compound bounds.
    if (intersect) {
        // Intersection is analogous to polynomial multiplication. Using '.' to denote intersection
        // and '+' to denote union. (a.b + c.d) . (e+f) = a.b.e + c.d.e + a.b.f + c.d.f
        // TODO: in certain cases we can simplify further. For example if we only have scalars, we
        // can simplify (-inf, 10) ^ (5, +inf) to (5, 10), but this does not work with arrays.

        IntervalRequirementDNF result;
        for (const auto& sourceConjunction : source) {
            // Specifically obtaining copies in the loop.
            for (auto targetConjunctionCopy : target) {
                // TODO: handle case with targetConjunct  fully open
                // TODO: handle case with targetConjunct half-open and sourceConjuct equality.
                // TODO: handle case with both targetConjunct and sourceConjuct equalities
                // (different consts).

                std::copy(sourceConjunction.cbegin(),
                          sourceConjunction.cend(),
                          std::back_inserter(targetConjunctionCopy));
                result.emplace_back(std::move(targetConjunctionCopy));
            }
        }
        target = result;
    } else {
        // Unioning is analogous to polynomial addition.
        // (a.b + c.d) + (e+f) = a.b + c.d + e + f
        for (const auto& sourceConjunction : source) {
            target.push_back(sourceConjunction);
        }
    }
}

bool combineMultiKeyIntervals(MultiKeyIntervalRequirementDNF& intervals,
                              const IntervalRequirementDNF& sourceIntervals) {
    MultiKeyIntervalRequirementDNF result;

    for (const auto& sourceConjunction : sourceIntervals) {
        for (const auto& targetConjunction : intervals) {
            std::vector<MultiKeyIntervalRequirement> newConjunction;
            for (const auto& sourceConjunct : sourceConjunction) {
                for (auto targetConjunctCopy : targetConjunction) {
                    if (!targetConjunctCopy.empty() && !targetConjunctCopy.back().isEquality() &&
                        !sourceConjunct.isFullyOpen()) {
                        return false;
                    }
                    targetConjunctCopy.push_back(sourceConjunct);
                    newConjunction.emplace_back(std::move(targetConjunctCopy));
                }
            }
            result.emplace_back(std::move(newConjunction));
        }
    }

    intervals = std::move(result);
    return true;
}

/**
 * Helper class that builds PartialSchemaRequirements property from an EvalFilter or an EvalPath.
 */
class PartialSchemaReqConverter {
public:
    PartialSchemaReqConverter() = default;

    PartialSchemaReqConversion handleEvalPathAndEvalFilter(PartialSchemaReqConversion pathResult,
                                                           PartialSchemaReqConversion inputResult) {
        if (!pathResult._success || !inputResult._success) {
            return {};
        }
        if (pathResult.hasBound() || !inputResult.hasBound() || !inputResult._reqMap.empty()) {
            return {};
        }

        if (auto boundPtr = inputResult._bound.cast<Variable>(); boundPtr != nullptr) {
            const ProjectionName& boundVarName = boundPtr->name();
            PartialSchemaRequirements newMap;

            for (auto& entry : pathResult._reqMap) {
                if (!entry.first._projectionName.empty()) {
                    return {};
                }
                newMap.emplace(PartialSchemaKey{boundVarName, entry.first._path},
                               std::move(entry.second));
            }

            return {std::move(newMap)};
        }

        return {};
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const EvalPath& evalPath,
                                         PartialSchemaReqConversion pathResult,
                                         PartialSchemaReqConversion inputResult) {
        return handleEvalPathAndEvalFilter(std::move(pathResult), std::move(inputResult));
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const EvalFilter& evalFilter,
                                         PartialSchemaReqConversion pathResult,
                                         PartialSchemaReqConversion inputResult) {
        return handleEvalPathAndEvalFilter(std::move(pathResult), std::move(inputResult));
    }

    static PartialSchemaReqConversion handleComposition(const bool isMultiplicative,
                                                        PartialSchemaReqConversion leftResult,
                                                        PartialSchemaReqConversion rightResult) {
        if (!leftResult._success || !rightResult._success) {
            return {};
        }
        if (leftResult.hasBound() || rightResult.hasBound()) {
            return {};
        }

        auto& leftReq = leftResult._reqMap;
        auto& rightReq = rightResult._reqMap;
        if (isMultiplicative) {
            if (!intersectPartialSchemaReq(leftReq, rightReq)) {
                return {};
            }
            return leftResult;
        }

        // We can only perform additive composition (OR) if we have a single matching key on both
        // sides.
        if (leftReq.size() != 1 || rightReq.size() != 1) {
            return {};
        }

        auto leftEntry = leftReq.begin();
        auto rightEntry = rightReq.begin();
        if (!(leftEntry->first == rightEntry->first)) {
            return {};
        }

        combineIntervals(
            isMultiplicative, leftEntry->second.getIntervals(), rightEntry->second.getIntervals());
        return leftResult;
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const PathComposeM& pathComposeM,
                                         PartialSchemaReqConversion leftResult,
                                         PartialSchemaReqConversion rightResult) {
        return handleComposition(
            true /*isMultiplicative*/, std::move(leftResult), std::move(rightResult));
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const PathComposeA& pathComposeA,
                                         PartialSchemaReqConversion leftResult,
                                         PartialSchemaReqConversion rightResult) {
        return handleComposition(
            false /*isMultiplicative*/, std::move(leftResult), std::move(rightResult));
    }

    template <class T>
    static PartialSchemaReqConversion handleGetAndTraverse(const ABT& n,
                                                           PartialSchemaReqConversion inputResult) {
        if (!inputResult._success) {
            return {};
        }
        if (inputResult.hasBound()) {
            return {};
        }

        // New map has keys with appended paths.
        PartialSchemaRequirements newMap;

        for (auto& entry : inputResult._reqMap) {
            const ProjectionName& projectionName = entry.first._projectionName;
            if (!projectionName.empty()) {
                return {};
            }

            ABT path = entry.first._path;

            // Updated key path to be now rooted at n, with existing key path as child.
            ABT appendedPath = n;
            std::swap(appendedPath.cast<T>()->getPath(), path);
            std::swap(path, appendedPath);

            newMap.emplace(PartialSchemaKey{projectionName, std::move(path)},
                           std::move(entry.second));
        }

        return {std::move(newMap)};
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const PathGet& pathGet,
                                         PartialSchemaReqConversion inputResult) {
        return handleGetAndTraverse<PathGet>(n, std::move(inputResult));
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const PathTraverse& pathTraverse,
                                         PartialSchemaReqConversion inputResult) {
        return handleGetAndTraverse<PathTraverse>(n, std::move(inputResult));
    }

    PartialSchemaReqConversion transport(const ABT& n,
                                         const PathCompare& pathCompare,
                                         PartialSchemaReqConversion inputResult) {
        if (!inputResult._success) {
            return {};
        }
        if (!inputResult.hasBound() || !inputResult._reqMap.empty()) {
            return {};
        }

        const ABT& bound = inputResult._bound;
        bool lowBoundInclusive = false;
        ABT lowBound = make<Blackhole>();
        bool highBoundInclusive = false;
        ABT highBound = make<Blackhole>();

        const Operations op = pathCompare.op();
        switch (op) {
            case Operations::Eq:
                lowBoundInclusive = true;
                lowBound = bound;
                highBoundInclusive = true;
                highBound = bound;
                break;

            case Operations::Lt:
            case Operations::Lte:
                lowBoundInclusive = false;
                highBoundInclusive = op == Operations::Lte;
                highBound = bound;
                break;

            case Operations::Gt:
            case Operations::Gte:
                lowBoundInclusive = op == Operations::Gte;
                lowBound = bound;
                highBoundInclusive = false;
                break;

            default:
                // TODO handle other comparisons?
                return {};
        }

        return {PartialSchemaRequirements{
            {PartialSchemaKey{},
             PartialSchemaRequirement{
                 "" /*boundProjectionName*/,
                 {{{{lowBoundInclusive, lowBound}, {highBoundInclusive, highBound}}}}}}}};
    }

    PartialSchemaReqConversion transport(const ABT& n, const PathIdentity& pathIdentity) {
        return {PartialSchemaRequirements{{{}, {}}}};
    }

    PartialSchemaReqConversion transport(const ABT& n, const Constant& exprConst) {
        return {n};
    }

    PartialSchemaReqConversion transport(const ABT& n, const Variable& exprVar) {
        return {n};
    }

    template <typename T, typename... Ts>
    PartialSchemaReqConversion transport(const ABT& /*n*/, const T& /*node*/, Ts&&...) {
        // General case. Reject conversion.
        return {};
    }

    PartialSchemaReqConversion convert(const ABT& input) {
        return algebra::transport<true>(input, *this);
    }
};

PartialSchemaReqConversion convertExprToPartialSchemaReq(const ABT& expr) {
    PartialSchemaReqConverter converter;
    PartialSchemaReqConversion result = converter.convert(expr);

    for (const auto& entry : result._reqMap) {
        if (entry.first.emptyPath() && entry.second.getIntervals() == kIntervalReqFullyOpenDNF) {
            // We need to determine either path or interval (or both).
            result._success = false;
            break;
        }
    }
    return result;
}

/**
 * Appends a path to another path. Performs the append at PathIdentity elements.
 */
class PathAppender {
public:
    PathAppender(ABT toAppend) : _toAppend(std::move(toAppend)) {}

    void transport(ABT& n, const PathIdentity& node) {
        n = _toAppend;
    }

    template <typename T, typename... Ts>
    void transport(ABT& /*n*/, const T& /*node*/, Ts&&...) {
        // noop
    }

    void append(ABT& path) {
        return algebra::transport<true>(path, *this);
    }

private:
    ABT _toAppend;
};

static bool mergeAppendPartialSchemaReq(PartialSchemaRequirements& reqMap,
                                        const PartialSchemaKey& key,
                                        const PartialSchemaRequirement& req) {
    if (!req.hasBoundProjectionName() || req.getIntervals() != kIntervalReqFullyOpenDNF) {
        return false;
    }

    bool result = false;

    // Try to merge with an entry which refers to us.
    for (auto it = reqMap.begin(); it != reqMap.cend();) {
        const auto& [existingKey, existingReq] = *it;
        if (existingKey._projectionName != req.getBoundProjectionName() ||
            !existingKey.emptyPath()) {
            it++;
            continue;
        }

        uassert(0, "Should not be merging with more than one empty path entry.", !result);
        uassert(
            0, "Empty path entry should not be binding.", !existingReq.hasBoundProjectionName());

        result = true;

        PartialSchemaKey mergedKey = key;

        // Append paths.
        PathAppender appender(existingKey._path);
        appender.append(mergedKey._path);

        PartialSchemaRequirement mergedReq = existingReq;
        if (mergedKey._path == key._path) {
            mergedReq.setBoundProjectionName(req.getBoundProjectionName());
        }

        it = reqMap.erase(it);
        reqMap.emplace(std::move(mergedKey), std::move(mergedReq));
    }

    return result;
}

static bool intersectPartialSchemaReq(PartialSchemaRequirements& reqMap,
                                      const PartialSchemaKey& key,
                                      const PartialSchemaRequirement& req) {
    auto it = reqMap.find(key);
    if (it == reqMap.cend()) {
        if (!mergeAppendPartialSchemaReq(reqMap, key, req)) {
            reqMap.emplace(key, req);
        }
        return true;
    }

    PartialSchemaRequirement& merged = it->second;

    if (req.hasBoundProjectionName()) {
        if (merged.hasBoundProjectionName()) {
            return false;
        }
        merged.setBoundProjectionName(req.getBoundProjectionName());
    }

    combineIntervals(true /*intersect*/, merged.getIntervals(), req.getIntervals());
    return true;
}

bool intersectPartialSchemaReq(PartialSchemaRequirements& target,
                               const PartialSchemaRequirements& source) {
    for (const auto& entry : source) {
        if (!intersectPartialSchemaReq(target, entry.first, entry.second)) {
            return false;
        }
    }

    // Validate merged map does not contain duplicate bound projections.
    ProjectionNameSet boundsProjectionNameSet;
    for (const auto& entry : target) {
        const ProjectionName& boundProjName = entry.second.getBoundProjectionName();
        if (!boundProjName.empty() && !boundsProjectionNameSet.insert(boundProjName).second) {
            uasserted(0, "Duplicate bound projection");
        }
    }

    return true;
}

std::string computeIndexKeyName(const size_t indexField) {
    std::ostringstream os;
    os << kIndexKeyPrefix << " " << indexField;
    return os.str();
}

CandidateIndexMap computeCandidateIndexMap(const ProjectionName& scanProjectionName,
                                           const PartialSchemaRequirements& reqMap,
                                           const ScanDefinition& scanDef) {
    CandidateIndexMap result;

    for (const auto& [indexDefName, indexDef] : scanDef.getIndexDefs()) {
        FieldProjectionMap indexProjectionMap;
        // Singular empty interval.
        MultiKeyIntervalRequirementDNF intervals = {{{}}};
        std::unordered_set<size_t> fieldsToCollate;

        std::unordered_set<PartialSchemaKey, PartialSchemaKeyHash> satisfiedKeys;

        // True if the paths from partial schema requirements form a strict prefix of the index
        // collation.
        bool isPrefix = true;
        bool indexSuitable = true;

        const IndexCollationSpec& indexCollationSpec = indexDef.getCollationSpec();
        for (size_t indexField = 0; indexField < indexCollationSpec.size(); indexField++) {
            const auto& indexCollationEntry = indexCollationSpec.at(indexField);
            PartialSchemaKey key{scanProjectionName, indexCollationEntry._path};

            auto it = reqMap.find(key);
            if (it == reqMap.cend()) {
                isPrefix = false;
                continue;
            }
            if (!isPrefix) {
                indexSuitable = false;
                break;
            }

            const PartialSchemaRequirement& req = it->second;
            const auto& requiredInterval = req.getIntervals();
            if (!combineMultiKeyIntervals(intervals, requiredInterval)) {
                indexSuitable = false;
                break;
            }

            if (req.hasBoundProjectionName()) {
                // Include bounds projection into index spec.
                indexProjectionMap._fieldProjections.emplace(computeIndexKeyName(indexField),
                                                             req.getBoundProjectionName());

                if (requiredInterval.size() != 1 || requiredInterval.at(0).size() != 1 ||
                    !requiredInterval.at(0).at(0).isEquality()) {
                    // We only care about collation of for non-equality intervals.
                    // Equivalently, it is sufficient for singular intervals to be clustered.
                    fieldsToCollate.insert(indexField);
                }
            }

            satisfiedKeys.emplace(std::move(key));
        }
        if (!indexSuitable) {
            continue;
        }

        if (const auto& partialReqMap = indexDef.getPartialReqMap(); !partialReqMap.empty()) {
            auto intersection = reqMap;
            if (!intersectPartialSchemaReq(intersection, partialReqMap) || intersection != reqMap) {
                // Query requirement is not subsumed into partial filter requirement.
                continue;
            }
            for (const auto& entry : partialReqMap) {
                satisfiedKeys.insert(entry.first);
            }
        }

        if (satisfiedKeys.size() < reqMap.size()) {
            continue;
        }

        result.emplace(indexDefName,
                       CandidateIndexEntry{std::move(indexProjectionMap),
                                           std::move(intervals),
                                           std::move(fieldsToCollate)});
    }

    return result;
}

static ABT createIntersectOrUnionForIndexLower(const bool isIntersect,
                                               ABTVector inputs,
                                               const FieldProjectionMap& innerMap,
                                               const FieldProjectionMap& outerMap,
                                               PrefixId& prefixId) {
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
        countProjection = prefixId.getNextId("count");
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

/**
 * Decomposes compound bound into a series of unions and intersections of singular intervals by
 * unrolling into explicit unions and intersects.
 * TODO: recursive CTE's to lower intervals with multiple inequalities (data-dependent bounds).
 */
static void indexIntervalDNFLower(ABT& n, const IndexScanNode& node, PrefixId& prefixId) {
    const auto& spec = node.getIndexSpecification();
    const auto& intervals = spec.getIntervals();
    const bool isSingularDisjunction = intervals.size() == 1;
    if (isSingularDisjunction && intervals.front().size() == 1) {
        // Already lowered.
        return;
    }

    FieldProjectionMap outerMap = node.getFieldProjectionMap();
    if (outerMap._ridProjection.empty()) {
        outerMap._ridProjection = prefixId.getNextId("rid");
    }
    if (!isSingularDisjunction) {
        for (auto& [fieldName, projectionName] : outerMap._fieldProjections) {
            projectionName = prefixId.getNextId("outer");
        }
    }

    ABTVector disjuncts;
    for (const auto& conjunction : intervals) {
        const bool isSingularConjunction = conjunction.size() == 1;
        FieldProjectionMap innerMap = outerMap;
        if (!isSingularConjunction) {
            for (auto& [fieldName, projectionName] : innerMap._fieldProjections) {
                projectionName = prefixId.getNextId("inner");
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
            true /*isIntersect*/, std::move(conjuncts), innerMap, outerMap, prefixId));
    }

    n = createIntersectOrUnionForIndexLower(false /*isIntersect*/,
                                            std::move(disjuncts),
                                            outerMap,
                                            node.getFieldProjectionMap(),
                                            prefixId);
}

class MemoPhysicalPlanExtractor {
public:
    explicit MemoPhysicalPlanExtractor(
        const cascades::Memo& memo,
        std::unordered_map<const Node*, MemoPhysicalNodeId>& nodeToPhysPropsMap,
        PrefixId& prefixId)
        : _memo(memo), _nodeToPhysPropsMap(nodeToPhysPropsMap), _prefixId(prefixId) {}

    /**
     * Physical delegator node.
     */
    void transport(ABT& n, const MemoPhysicalDelegatorNode& node, const MemoPhysicalNodeId /*id*/) {
        n = extract(node.getNodeId());
    }

    /**
     * TODO: consider moving into its own rewrite.
     */
    void transport(ABT& n,
                   const IndexScanNode& node,
                   const MemoPhysicalNodeId id,
                   ABT& /*binder*/) {
        indexIntervalDNFLower(n, node, _prefixId);

        // For now include only the root node of the translation.
        _nodeToPhysPropsMap.emplace(n.cast<Node>(), id);
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    void transport(ABT& /*n*/, const T& node, const MemoPhysicalNodeId id, Ts&&...) {
        if constexpr (std::is_base_of_v<Node, T>) {
            _nodeToPhysPropsMap.emplace(&node, id);
        }
    }

    ABT extract(const MemoPhysicalNodeId nodeId) {
        ABT node = _memo.getGroup(nodeId._groupId)._physicalNodes.at(nodeId._index)->_node;
        algebra::transport<true>(node, *this, nodeId);
        return node;
    }

private:
    // We don't own this.
    const cascades::Memo& _memo;
    std::unordered_map<const Node*, MemoPhysicalNodeId>& _nodeToPhysPropsMap;
    PrefixId& _prefixId;
};

std::pair<ABT, std::unordered_map<const Node*, MemoPhysicalNodeId>> extractPhysicalPlan(
    const MemoPhysicalNodeId id, const cascades::Memo& memo, PrefixId& prefixId) {
    std::unordered_map<const Node*, MemoPhysicalNodeId> resultMap;
    MemoPhysicalPlanExtractor extractor(memo, resultMap, prefixId);
    ABT resultNode = extractor.extract(id);
    return {std::move(resultNode), std::move(resultMap)};
}

}  // namespace mongo::optimizer
