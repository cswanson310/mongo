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

#include "mongo/db/query/optimizer/abt_hash.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer {

/**
 * Specialized hasher which may optionally skip including child hashes.
 * Used in conjunction with memo.
 *
 * Binary and N-ary nodes include their children on purpose because of associativity.
 * If invoked from the memo, the children are memo logical delegators.
 */
template <bool includeChildNodes>
class ABTHashTransporter {
public:
    /**
     * Nodes
     */
    template <typename T, typename... Ts>
    size_t transport(const T& /*node*/, Ts&&...) {
        // Physical nodes do not currently need to implement hash.
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its hash.");
        uasserted(0, "must implement custom hash");
    }

    size_t transport(const References& references, std::vector<size_t> inResults) {
        return computeHashSeq<1>(computeVectorHash(inResults));
    }

    size_t transport(const ExpressionBinder& binders, std::vector<size_t> inResults) {
        return computeHashSeq<2>(computeVectorHash(binders.names()), computeVectorHash(inResults));
    }

    size_t transport(const ScanNode& node, size_t bindResult) {
        return computeHashSeq<3>(std::hash<std::string>()(node.getScanDefName()), bindResult);
    }

    size_t transport(const MemoLogicalDelegatorNode& node) {
        return computeHashSeq<4>(std::hash<GroupIdType>()(node.getGroupId()));
    }

    size_t transport(const FilterNode& node, size_t childResult, size_t filterResult) {
        return computeHashSeq<5>(filterResult, includeChildNodes ? childResult : 0);
    }

    size_t transport(const EvaluationNode& node, size_t childResult, size_t projectionResult) {
        return computeHashSeq<6>(projectionResult, includeChildNodes ? childResult : 0);
    }

    static void updateBoundHash(size_t& result, const BoundRequirement& bound) {
        updateHash(result, std::hash<bool>()(bound.isInclusive()));
        if (!bound.isInfinite()) {
            updateHash(result, ABTHashGenerator::generateIncludeChildren(bound.getBound()));
        }
    };

    static void updateIntervalHash(size_t& result, const IntervalRequirement& interval) {
        updateBoundHash(result, interval.getLowBound());
        updateBoundHash(result, interval.getHighBound());
    }

    static size_t computePartialSchemaReqHash(const PartialSchemaRequirements& reqMap) {
        size_t result = 17;

        for (const auto& [key, req] : reqMap) {
            updateHash(result, std::hash<ProjectionName>()(key._projectionName));
            updateHash(result, ABTHashGenerator::generateIncludeChildren(key._path));
            updateHash(result, std::hash<ProjectionName>()(req.getBoundProjectionName()));

            for (const auto& disjunct : req.getIntervals()) {
                for (const auto& conjunct : disjunct) {
                    updateIntervalHash(result, conjunct);
                }
            }
        }
        return result;
    }

    static size_t computeCandidateIndexMapHash(const CandidateIndexMap& map) {
        size_t result = 17;

        for (const auto& [indexDefName, candidateIndexEntry] : map) {
            updateHash(result, std::hash<std::string>()(indexDefName));

            {
                const auto& fieldProjectionMap = candidateIndexEntry._fieldProjectionMap;
                updateHash(result, std::hash<ProjectionName>()(fieldProjectionMap._ridProjection));
                updateHash(result, std::hash<ProjectionName>()(fieldProjectionMap._rootProjection));
                for (const auto& fieldProjectionMapEntry : fieldProjectionMap._fieldProjections) {
                    updateHash(result, std::hash<FieldNameType>()(fieldProjectionMapEntry.first));
                    updateHash(result, std::hash<ProjectionName>()(fieldProjectionMapEntry.second));
                }
            }

            for (const auto& disjunct : candidateIndexEntry._intervals) {
                for (const auto& conjunct : disjunct) {
                    for (const auto& interval : conjunct) {
                        updateIntervalHash(result, interval);
                    }
                }
            }
        }

        return result;
    }

    size_t transport(const SargableNode& node,
                     size_t childResult,
                     size_t /*bindResult*/,
                     size_t /*refResult*/) {
        return computeHashSeq<44>(computePartialSchemaReqHash(node.getReqMap()),
                                  computeCandidateIndexMapHash(node.getCandidateIndexMap()),
                                  includeChildNodes ? childResult : 0);
    }

    size_t transport(const RIDIntersectNode& node,
                     size_t leftChildResult,
                     size_t rightChildResult) {
        // Specifically always including children.
        return computeHashSeq<45>(std::hash<std::string>()(node.getScanProjectionName()),
                                  leftChildResult,
                                  rightChildResult);
    }

    size_t transport(const BinaryJoinNode& node,
                     size_t leftChildResult,
                     size_t rightChildResult,
                     size_t filterResult) {
        // Specifically always including children.
        return computeHashSeq<7>(filterResult, leftChildResult, rightChildResult);
    }

    size_t transport(const InnerMultiJoinNode& node, std::vector<size_t> childResults) {
        // Specifically always including children.
        return computeHashSeq<8>(computeVectorHash(childResults));
    }

    size_t transport(const UnionNode& node,
                     std::vector<size_t> childResults,
                     size_t bindResult,
                     size_t refsResult) {
        // Specifically always including children.
        return computeHashSeq<9>(bindResult, refsResult, computeVectorHash(childResults));
    }

    size_t transport(const GroupByNode& node,
                     size_t childResult,
                     size_t bindAggResult,
                     size_t refsAggResult,
                     size_t bindGbResult,
                     size_t refsGbResult) {
        return computeHashSeq<10>(bindAggResult,
                                  refsAggResult,
                                  bindGbResult,
                                  refsGbResult,
                                  std::hash<bool>()(node.isLocal()),
                                  std::hash<bool>()(node.canRewriteIntoLocal()),
                                  includeChildNodes ? childResult : 0);
    }

    size_t transport(const UnwindNode& node,
                     size_t childResult,
                     size_t bindResult,
                     size_t refsResult) {
        return computeHashSeq<11>(std::hash<bool>()(node.getRetainNonArrays()),
                                  bindResult,
                                  refsResult,
                                  includeChildNodes ? childResult : 0);
    }

    size_t transport(const WindNode& node,
                     size_t childResult,
                     size_t bindResult,
                     size_t refsResult) {
        return computeHashSeq<12>(bindResult, refsResult, includeChildNodes ? childResult : 0);
    }

    static size_t computeCollationHash(const properties::CollationRequirement& prop) {
        size_t collationHash = 17;
        for (const auto& entry : prop.getCollationSpec()) {
            updateHash(collationHash, std::hash<std::string>()(entry.first));
            updateHash(collationHash, std::hash<CollationOp>()(entry.second));
        }
        return collationHash;
    }

    size_t transport(const CollationNode& node, size_t childResult, size_t /*refsResult*/) {
        return computeHashSeq<13>(computeCollationHash(node.getProperty()),
                                  includeChildNodes ? childResult : 0);
    }

    static size_t computeLimitSkipHash(const properties::LimitSkipRequirement& prop) {
        size_t limitSkipHash = 17;
        updateHash(limitSkipHash,
                   std::hash<properties::LimitSkipRequirement::IntType>()(prop.getLimit()));
        updateHash(limitSkipHash,
                   std::hash<properties::LimitSkipRequirement::IntType>()(prop.getSkip()));
        updateHash(limitSkipHash, std::hash<bool>()(prop.isEnforced()));
        return limitSkipHash;
    }

    size_t transport(const LimitSkipNode& node, size_t childResult) {
        return computeHashSeq<14>(computeLimitSkipHash(node.getProperty()),
                                  includeChildNodes ? childResult : 0);
    }

    static size_t computePropertyProjectionsHash(const ProjectionNameVector& projections) {
        size_t resultHash = 17;
        for (const ProjectionName& projection : projections) {
            updateHash(resultHash, std::hash<std::string>()(projection));
        }
        return resultHash;
    }

    static size_t computeDistributionHash(const properties::DistributionRequirement& prop) {
        size_t resultHash = 17;
        updateHash(resultHash, std::hash<DistributionType>()(prop.getType()));
        updateHash(resultHash, computePropertyProjectionsHash(prop.getProjections()));
        return resultHash;
    }

    size_t transport(const ExchangeNode& node, size_t childResult, size_t /*refsResult*/) {
        return computeHashSeq<43>(computeDistributionHash(node.getProperty()),
                                  std::hash<bool>()(node.getPreserveSort()),
                                  includeChildNodes ? childResult : 0);
    }

    static size_t computeProjectionRequirementHash(const properties::ProjectionRequirement& prop) {
        return computePropertyProjectionsHash(prop.getProjections().getVector());
    }

    size_t transport(const RootNode& node, size_t childResult, size_t /*refsResult*/) {
        return computeHashSeq<15>(computeProjectionRequirementHash(node.getProperty()),
                                  includeChildNodes ? childResult : 0);
    }

    /**
     * Expressions
     */
    size_t transport(const Blackhole& expr) {
        return computeHashSeq<16>();
    }

    size_t transport(const Constant& expr) {
        auto [tag, val] = expr.get();
        return computeHashSeq<17>(sbe::value::hashValue(tag, val));
    }

    size_t transport(const Variable& expr) {
        return computeHashSeq<18>(std::hash<std::string>()(expr.name()));
    }

    size_t transport(const UnaryOp& expr, size_t inResult) {
        return computeHashSeq<19>(std::hash<Operations>()(expr.op()), inResult);
    }

    size_t transport(const BinaryOp& expr, size_t leftResult, size_t rightResult) {
        return computeHashSeq<20>(std::hash<Operations>()(expr.op()), leftResult, rightResult);
    }

    size_t transport(const If& expr, size_t condResult, size_t thenResult, size_t elseResult) {
        return computeHashSeq<21>(condResult, thenResult, elseResult);
    }

    size_t transport(const Let& expr, size_t bindResult, size_t exprResult) {
        return computeHashSeq<22>(std::hash<std::string>()(expr.varName()), bindResult, exprResult);
    }

    size_t transport(const LambdaAbstraction& expr, size_t inResult) {
        return computeHashSeq<23>(std::hash<std::string>()(expr.varName()), inResult);
    }

    size_t transport(const LambdaApplication& expr, size_t lambdaResult, size_t argumentResult) {
        return computeHashSeq<24>(lambdaResult, argumentResult);
    }

    size_t transport(const FunctionCall& expr, std::vector<size_t> argResults) {
        return computeHashSeq<25>(std::hash<std::string>()(expr.name()),
                                  computeVectorHash(argResults));
    }

    size_t transport(const EvalPath& expr, size_t pathResult, size_t inputResult) {
        return computeHashSeq<26>(pathResult, inputResult);
    }

    size_t transport(const EvalFilter& expr, size_t pathResult, size_t inputResult) {
        return computeHashSeq<27>(pathResult, inputResult);
    }

    size_t transport(const Source& expr) {
        return computeHashSeq<28>();
    }

    /**
     * Paths
     */
    size_t transport(const PathConstant& path, size_t inResult) {
        return computeHashSeq<29>(inResult);
    }

    size_t transport(const PathLambda& path, size_t inResult) {
        return computeHashSeq<30>(inResult);
    }

    size_t transport(const PathIdentity& path) {
        return computeHashSeq<31>();
    }

    size_t transport(const PathDefault& path, size_t inResult) {
        return computeHashSeq<32>(inResult);
    }

    size_t transport(const PathCompare& path, size_t valueResult) {
        return computeHashSeq<33>(std::hash<Operations>()(path.op()), valueResult);
    }

    size_t transport(const PathDrop& path) {
        size_t namesHash = 17;
        for (const std::string& name : path.getNames()) {
            updateHash(namesHash, std::hash<std::string>()(name));
        }
        return computeHashSeq<34>(namesHash);
    }

    size_t transport(const PathKeep& path) {
        size_t namesHash = 17;
        for (const std::string& name : path.getNames()) {
            updateHash(namesHash, std::hash<std::string>()(name));
        }
        return computeHashSeq<35>(namesHash);
    }

    size_t transport(const PathObj& path) {
        return computeHashSeq<36>();
    }

    size_t transport(const PathArr& path) {
        return computeHashSeq<37>();
    }

    size_t transport(const PathTraverse& path, size_t inResult) {
        return computeHashSeq<38>(inResult);
    }

    size_t transport(const PathField& path, size_t inResult) {
        return computeHashSeq<39>(std::hash<std::string>()(path.name()), inResult);
    }

    size_t transport(const PathGet& path, size_t inResult) {
        return computeHashSeq<40>(std::hash<std::string>()(path.name()), inResult);
    }

    size_t transport(const PathComposeM& path, size_t leftResult, size_t rightResult) {
        return computeHashSeq<41>(leftResult, rightResult);
    }

    size_t transport(const PathComposeA& path, size_t leftResult, size_t rightResult) {
        return computeHashSeq<42>(leftResult, rightResult);
    }

    size_t generate(const ABT& node) {
        return algebra::transport<false>(node, *this);
    }

    size_t generate(const ABT::reference_type& nodeRef) {
        return algebra::transport<false>(nodeRef, *this);
    }
};

size_t ABTHashGenerator::generateIncludeChildren(const ABT& node) {
    ABTHashTransporter<true> gen;
    return gen.generate(node);
}

size_t ABTHashGenerator::generateIncludeChildrenRef(const ABT::reference_type& nodeRef) {
    ABTHashTransporter<true> gen;
    return gen.generate(nodeRef);
}

size_t ABTHashGenerator::generateNoChildren(const ABT& node) {
    ABTHashTransporter<false> gen;
    return gen.generate(node);
}

size_t ABTHashGenerator::generateNoChildrenRef(const ABT::reference_type& nodeRef) {
    ABTHashTransporter<false> gen;
    return gen.generate(nodeRef);
}

}  // namespace mongo::optimizer
