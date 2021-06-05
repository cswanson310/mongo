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

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mongo/db/query/optimizer/algebra/operator.h"
#include "mongo/db/query/optimizer/defs.h"
#include "mongo/db/query/optimizer/metadata.h"
#include "mongo/db/query/optimizer/props.h"
#include "mongo/db/query/optimizer/syntax/expr.h"
#include "mongo/db/query/optimizer/syntax/path.h"


namespace mongo::optimizer {

using FilterType = ABT;
using ProjectionType = ABT;

/**
 * Marker for node class (both logical and physical sub-classes).
 * A node not marked with either LogicalNode or PhysicalNode is considered to be both a logical and
 * a physical node (e.g. a filter node). It is invalid to mark a node with both tags in the same
 * time.
 */
class Node {};

/**
 * Marker for exclusively logical nodes.
 */
class LogicalNode {};

/**
 * Marker for exclusively physical nodes.
 * TODO: implement physical nodes.
 */
class PhysicalNode {};

inline void assertNodeSort(const ABT& e) {
    if (!e.is<Node>()) {
        uasserted(6624009, "Node syntax sort expected");
    }
}

template <class T>
inline constexpr bool canBeLogicalNode() {
    // Node which is not exclusively physical.
    return std::is_base_of_v<Node, T> && !std::is_base_of_v<PhysicalNode, T>;
}

template <class T>
inline constexpr bool canBePhysicalNode() {
    // Node which is not exclusively logical.
    return std::is_base_of_v<Node, T> && !std::is_base_of_v<LogicalNode, T>;
}

/**
 * Logical Scan node.
 * It defines scanning a collection with an optional projection name that contains the documents.
 * The collection is specified via a ScanSpecification.
 */
class ScanNode final : public Operator<ScanNode, 1>, public Node, public LogicalNode {
    using Base = Operator<ScanNode, 1>;

public:
    static constexpr const char* kDefaultCollectionNameSpec = "collectionName";

    ScanNode(ProjectionName projectionName, std::string scanDefName);

    bool operator==(const ScanNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<0>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionName& getProjectionName() const;
    const ProjectionType& getProjection() const;

    const std::string& getScanDefName() const;

private:
    const std::string _scanDefName;
};

/**
 * Physical Scan node.
 * It defines scanning a collection with an optional projection name that contains the documents.
 * The collection is specified via a ScanSpecification.
 *
 * Optionally set of fields is specified to retrieve from the underlying collection, and expose as
 * projections.
 */
class PhysicalScanNode final : public Operator<PhysicalScanNode, 1>,
                               public Node,
                               public PhysicalNode {
    using Base = Operator<PhysicalScanNode, 1>;

public:
    PhysicalScanNode(FieldProjectionMap fieldProjectionMap,
                     std::string scanDefName,
                     bool useParallelScan);

    bool operator==(const PhysicalScanNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<0>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const FieldProjectionMap& getFieldProjectionMap() const;

    const std::string& getScanDefName() const;

    bool useParallelScan() const;

private:
    const FieldProjectionMap _fieldProjectionMap;
    const std::string _scanDefName;
    const bool _useParallelScan;
};

class CoScanNode final : public Operator<CoScanNode, 0>, public Node, public PhysicalNode {
    using Base = Operator<CoScanNode, 0>;

public:
    CoScanNode();

    bool operator==(const CoScanNode& other) const;
};

/**
 * Index scan node.
 * Retrieve data using an index. Return recordIds or values (if the index is covering).
 * This is a physical node.
 */
class IndexScanNode final : public Operator<IndexScanNode, 1>, public Node, public PhysicalNode {
    using Base = Operator<IndexScanNode, 1>;

public:
    IndexScanNode(FieldProjectionMap fieldProjectionMap, IndexSpecification indexSpec);

    bool operator==(const IndexScanNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<0>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const FieldProjectionMap& getFieldProjectionMap() const;
    const IndexSpecification& getIndexSpecification() const;

private:
    const FieldProjectionMap _fieldProjectionMap;
    const IndexSpecification _indexSpec;
};

/**
 * SeekNode.
 * Retrieve values using rowIds (typically previously retrieved using an index scan).
 * This is a physical node.
 *
 * 'ridProjectionName' parameter designates the incoming rid which is the starting point of the
 * seek. 'fieldProjectionMap' may choose to include an outgoing rid which will contain the
 * successive (if we do not have a following limit) document ids.
 *
 * TODO: Seek is currently lowered to ScanNode with Limit(1) to prevent it from advancing.
 * TODO: Can we let it advance with a limit based on upper rid limit in case of primary index?
 */
class SeekNode final : public Operator<SeekNode, 2>, public Node, public PhysicalNode {
    using Base = Operator<SeekNode, 2>;

public:
    SeekNode(ProjectionName ridProjectionName,
             FieldProjectionMap fieldProjectionMap,
             std::string scanDefName);

    bool operator==(const SeekNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<0>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionName& getRIDProjectionName() const;

    const FieldProjectionMap& getFieldProjectionMap() const;

    const std::string& getScanDefName() const;

private:
    const ProjectionName _ridProjectionName;
    const FieldProjectionMap _fieldProjectionMap;
    const std::string _scanDefName;
};


/**
 * Logical group delegator node: scan from a given group.
 * Used in conjunction with memo.
 */
class MemoLogicalDelegatorNode final : public Operator<MemoLogicalDelegatorNode, 0>,
                                       public Node,
                                       public LogicalNode {
    using Base = Operator<MemoLogicalDelegatorNode, 0>;

public:
    MemoLogicalDelegatorNode(GroupIdType groupId);

    bool operator==(const MemoLogicalDelegatorNode& other) const;

    GroupIdType getGroupId() const;

private:
    const GroupIdType _groupId;
};

/**
 * Logical group delegator node: refer to a physical node in a memo group.
 * Used in conjunction with memo.
 */
class MemoPhysicalDelegatorNode final : public Operator<MemoPhysicalDelegatorNode, 0>,
                                        public Node,
                                        public PhysicalNode {
    using Base = Operator<MemoPhysicalDelegatorNode, 0>;

public:
    MemoPhysicalDelegatorNode(MemoPhysicalNodeId nodeId);

    bool operator==(const MemoPhysicalDelegatorNode& other) const;

    MemoPhysicalNodeId getNodeId() const;

private:
    const MemoPhysicalNodeId _nodeId;
};

/**
 * Filter node.
 * It applies a filter over its input.
 *
 * This node is both logical and physical.
 */
class FilterNode final : public Operator<FilterNode, 2>, public Node {
    using Base = Operator<FilterNode, 2>;

public:
    FilterNode(FilterType filter, ABT child);
    FilterNode(const bool isInputVarTemp, FilterType filter, ABT child);

    bool operator==(const FilterNode& other) const;

    bool isInputVarTemp() const;
    void clearInputVarTemp();

    const FilterType& getFilter() const;
    FilterType& getFilter();

    const ABT& getChild() const;
    ABT& getChild();

private:
    // For logical filters only: Optionally prevent propagating to parent nodes the input variable
    // to EvalFilter. This is used to localize usage of a temporary variable introduced during
    // filter de-composition.
    bool _inputVarIsTemp;
};

/**
 * Evaluation node.
 * Adds a new projection to its input.
 *
 * This node is both logical and physical.
 */
class EvaluationNode final : public Operator<EvaluationNode, 2>, public Node {
    using Base = Operator<EvaluationNode, 2>;

public:
    EvaluationNode(ProjectionName projectionName, ProjectionType projection, ABT child);

    bool operator==(const EvaluationNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<1>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionName& getProjectionName() const {
        return binder().names()[0];
    }

    const ProjectionType& getProjection() const {
        return binder().exprs()[0];
    }

    const ABT& getChild() const {
        return get<0>();
    }

    ABT& getChild() {
        return get<0>();
    }
};

/**
 * RID intersection node.
 * This is a logical node representing index interval intersection.
 *
 * It is equivalent to a join node with the difference that RID projections do not exist on logical
 * level, and thus projection names are not determined until physical optimization. We want to also
 * restrict the type of operations on RIDs (in this case only set intersection) as opposed to say
 * filter on rid = 5.
 */
class RIDIntersectNode final : public Operator<RIDIntersectNode, 2>,
                               public Node,
                               public LogicalNode {
    using Base = Operator<RIDIntersectNode, 2>;

public:
    RIDIntersectNode(ProjectionName scanProjectionName, ABT leftChild, ABT rightChild);

    bool operator==(const RIDIntersectNode& other) const;

    const ABT& getLeftChild() const;
    ABT& getLeftChild();

    const ABT& getRightChild() const;
    ABT& getRightChild();

    const ProjectionName& getScanProjectionName() const;

private:
    const ProjectionName _scanProjectionName;
};

/**
 * Sargable node.
 * This is a logical node which represents special kinds of (simple) evaluations and filters which
 * are amenable to being used in indexing or covered scans.
 */
class SargableNode final : public Operator<SargableNode, 3>, public Node, public LogicalNode {
    using Base = Operator<SargableNode, 3>;

public:
    SargableNode(PartialSchemaRequirements reqMap, CandidateIndexMap candidateIndexMap, ABT child);

    bool operator==(const SargableNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<1>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ABT& getChild() const {
        return get<0>();
    }

    ABT& getChild() {
        return get<0>();
    }

    const PartialSchemaRequirements& getReqMap() const;
    const CandidateIndexMap& getCandidateIndexMap() const;

private:
    const PartialSchemaRequirements _reqMap;

    CandidateIndexMap _candidateIndexMap;
};

#define JOIN_TYPE(F) \
    F(Inner)         \
    F(Left)          \
    F(Right)         \
    F(Full)

MAKE_PRINTABLE_ENUM(JoinType, JOIN_TYPE);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(JoinTypeEnum, JoinType, JOIN_TYPE);
#undef JOIN_TYPE

/**
 * Logical binary join.
 * Join of two logical nodes. Can express inner and outer joins, with an associated join predicate.
 *
 * This node is logical, with a default physical implementation corresponding to a Nested Loops Join
 * (NLJ).
 * Variables used in the inner (right) side are automatically bound with variables from the left
 * (outer) side.
 *
 * TODO: other physical implementations: stream join.
 */
class BinaryJoinNode final : public Operator<BinaryJoinNode, 3>, public Node {
    using Base = Operator<BinaryJoinNode, 3>;

public:
    BinaryJoinNode(JoinType joinType,
                   ProjectionNameSet correlatedProjectionNames,
                   FilterType filter,
                   ABT leftChild,
                   ABT rightChild);

    bool operator==(const BinaryJoinNode& other) const;

    JoinType getJoinType() const;

    const ProjectionNameSet& getCorrelatedProjectionNames() const;

    const ABT& getLeftChild() const;
    ABT& getLeftChild();

    const ABT& getRightChild() const;
    ABT& getRightChild();

    const ABT& getFilter() const;

private:
    const JoinType _joinType;

    // Those projections must exist on the outer side and are used to bind free variables on the
    // inner side.
    const ProjectionNameSet _correlatedProjectionNames;
};

/**
 * Physical hash join node.
 * Join condition is a conjunction of pairwise equalities between corresponding left and right keys.
 */
class HashJoinNode final : public Operator<HashJoinNode, 3>, public Node, public PhysicalNode {
    using Base = Operator<HashJoinNode, 3>;

public:
    HashJoinNode(JoinType joinType,
                 ProjectionNameVector leftKeys,
                 ProjectionNameVector rightKeys,
                 ABT leftChild,
                 ABT rightChild);

    bool operator==(const HashJoinNode& other) const;

    JoinType getJoinType() const;
    const ProjectionNameVector& getLeftKeys() const;
    const ProjectionNameVector& getRightKeys() const;

    const ABT& getLeftChild() const;
    ABT& getLeftChild();

    const ABT& getRightChild() const;
    ABT& getRightChild();

private:
    const JoinType _joinType;

    // Join condition is a conjunction of _leftKeys.at(i) == _rightKeys.at(i).
    const ProjectionNameVector _leftKeys;
    const ProjectionNameVector _rightKeys;
};

/**
 * Logical inner multi-join.
 * Join of several logical nodes. Join is inner and it freely commutes. No explicit join condition
 * (cross product). No correlations.
 *
 * TODO: rewrite a series of BinaryJoins to a single InnerMultiJoin (translation will create a
 * BinaryJoinNode).
 */
class InnerMultiJoinNode final : public OperatorDynamicHomogenous<InnerMultiJoinNode>,
                                 public Node,
                                 public LogicalNode {
    using Base = OperatorDynamicHomogenous<InnerMultiJoinNode>;

public:
    InnerMultiJoinNode(ABTVector children);

    bool operator==(const InnerMultiJoinNode& other) const;
};

/**
 * Union of several logical nodes. Projections in common to all nodes are logically union-ed in the
 * output.
 *
 * This node is both logical and physical.
 */
class UnionNode final : public OperatorDynamic<UnionNode, 2>, public Node {
    using Base = OperatorDynamic<UnionNode, 2>;

public:
    UnionNode(ProjectionNameVector unionProjectionNames, ABTVector children);

    bool operator==(const UnionNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<0>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }
};

/**
 * Group-by node.
 * For now it is both logical and physical (eventually hashgb and streaming).
 * Projects the group-by column from its child, and adds aggregation expressions.
 *
 * This node is logical with a default physical implementation corresponding to a hash group-by.
 * TODO: other physical implementations: stream group-by.
 */
class GroupByNode : public Operator<GroupByNode, 5>, public Node {
    using Base = Operator<GroupByNode, 5>;

public:
    GroupByNode(ProjectionNameVector groupByProjectionNames,
                ProjectionNameVector aggregationProjectionNames,
                ABTVector aggregationExpressions,
                ABT child);

    GroupByNode(ProjectionNameVector groupByProjectionNames,
                ProjectionNameVector aggregationProjectionNames,
                ABTVector aggregationExpressions,
                bool isLocal,
                ABT child);

    GroupByNode(ProjectionNameVector groupByProjectionNames,
                ProjectionNameVector aggregationProjectionNames,
                ABTVector aggregationExpressions,
                bool isLocal,
                bool canRewriteIntoLocal,
                ABT child);

    bool operator==(const GroupByNode& other) const;

    const ExpressionBinder& binderAgg() const {
        const ABT& result = get<1>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ExpressionBinder& binderGb() const {
        const ABT& result = get<3>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionNameVector& getGroupByProjectionNames() const {
        return binderGb().names();
    }

    const ProjectionNameVector& getAggregationProjectionNames() const {
        return binderAgg().names();
    }

    const auto& getAggregationProjections() const {
        return binderAgg().exprs();
    }

    const auto& getGroupByProjections() const {
        return binderGb().exprs();
    }

    const ABTVector& getAggregationExpressions() const;

    const ABT& getChild() const;

    ABT& getChild();

    bool isLocal() const;
    bool canRewriteIntoLocal() const;

private:
    // Used for local-global rewrite.
    bool _isLocal;
    bool _canRewriteIntoLocal;
};

/**
 * Unwind node.
 * Unwinds an embedded relation inside an array. Generates unwinding positions in the CID
 * projection.
 *
 * This node is both logical and physical.
 */
class UnwindNode final : public Operator<UnwindNode, 3>, public Node {
    using Base = Operator<UnwindNode, 3>;

public:
    UnwindNode(ProjectionName projectionName,
               ProjectionName pidProjectionName,
               bool retainNonArrays,
               ABT child);

    bool operator==(const UnwindNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<1>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionName& getProjectionName() const {
        return binder().names()[0];
    }

    const ProjectionName& getPIDProjectionName() const {
        return binder().names()[1];
    }

    const ProjectionType& getProjection() const {
        return binder().exprs()[0];
    }

    const ProjectionType& getPIDProjection() const {
        return binder().exprs()[1];
    }

    const ABT& getChild() const;

    ABT& getChild();

    bool getRetainNonArrays() const;

private:
    const bool _retainNonArrays;
};

/**
 * Logical wind node.
 * Creates an embedded relation based on a projection. The embedding is done according to the PID
 * and CID supplementary projections.
 *
 * This node is both logical with a default physical implementation corresponding to a hash wind
 * (hash group-by).
 * TODO: stream wind.
 */
class WindNode final : public Operator<WindNode, 3>, public Node {
    using Base = Operator<WindNode, 3>;

public:
    WindNode(ProjectionName projectionName,
             ProjectionName pidProjectionName,
             ProjectionName cidProjectionName,
             ABT child);

    bool operator==(const WindNode& other) const;

    const ExpressionBinder& binder() const {
        const ABT& result = get<1>();
        uassert(6624000, "Invalid binder type", result.is<ExpressionBinder>());
        return *result.cast<ExpressionBinder>();
    }

    const ProjectionName& getProjectionName() const {
        return binder().names()[0];
    }

    const ProjectionType& getProjection() const {
        return binder().exprs()[0];
    }

    const ABT& getChild() const;
    ABT& getChild();
};

/**
 * Collation node.
 *
 * This node is both logical and physical.
 */
class CollationNode final : public Operator<CollationNode, 2>, public Node {
    using Base = Operator<CollationNode, 2>;

public:
    CollationNode(properties::CollationRequirement property, ABT child);

    bool operator==(const CollationNode& other) const;

    const properties::CollationRequirement& getProperty() const;
    properties::CollationRequirement& getProperty();

    const ABT& getChild() const;

    ABT& getChild();

private:
    properties::CollationRequirement _property;
};

/**
 * Limit and skip node.
 *
 * This node is both logical and physical.
 */
class LimitSkipNode final : public Operator<LimitSkipNode, 1>, public Node {
    using Base = Operator<LimitSkipNode, 1>;

public:
    LimitSkipNode(properties::LimitSkipRequirement property, ABT child);

    bool operator==(const LimitSkipNode& other) const;

    const properties::LimitSkipRequirement& getProperty() const;
    properties::LimitSkipRequirement& getProperty();

    const ABT& getChild() const;

    ABT& getChild();

private:
    properties::LimitSkipRequirement _property;
};

/**
 * Exchange node.
 * It specifies how the relation is spread across machines in the execution environment.
 * Currently only single-node, and hash-based partitioning are supported.
 * TODO: range-based partitioning, replication, and round-robin.
 *
 * This node is both logical and physical.
 */
class ExchangeNode final : public Operator<ExchangeNode, 2>, public Node {
    using Base = Operator<ExchangeNode, 2>;

public:
    ExchangeNode(properties::DistributionRequirement distribution, bool preserveSort, ABT child);

    bool operator==(const ExchangeNode& other) const;

    const properties::DistributionRequirement& getProperty() const;

    bool getPreserveSort() const;

    const ABT& getChild() const;

    ABT& getChild();

private:
    const properties::DistributionRequirement _distribution;

    /**
     * Defined for hash and range-based partitioning.
     * TODO: other exchange-specific params (e.g. chunk boundaries?)
     */
    const ProjectionName _projectionName;

    const bool _preserveSort;
};

/**
 * Root of the tree that holds references to the output of the query. In the mql case the query
 * outputs a single "column" (aka document) but in a general case (SQL) we can output arbitrary many
 * "columns". We need the internal references for the output projections in order to keep them live,
 * otherwise they would be dropped from the tree by DCE.
 *
 * This node is only logical.
 */
class RootNode final : public Operator<RootNode, 2>, public Node {
    using Base = Operator<RootNode, 2>;

public:
    RootNode(properties::ProjectionRequirement property, ABT child);

    bool operator==(const RootNode& other) const;

    const properties::ProjectionRequirement& getProperty() const;

    const ABT& getChild() const;
    ABT& getChild();

private:
    const properties::ProjectionRequirement _property;
};
}  // namespace mongo::optimizer
