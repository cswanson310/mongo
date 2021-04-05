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

#include "mongo/db/query/optimizer/abt_compare.h"
#include "mongo/db/query/optimizer/node.h"

namespace mongo::optimizer {

/**
 * Specialized comparator which can optionally skip comparison for child nodes.
 * Used in conjunction with memo.
 *
 * Binary and N-ary nodes include their children on purpose because of associativity.
 * If invoked from the memo, the children are memo logical delegators.
 */
template <bool includeChildNodes>
class ABTCompareTransporter {
public:
    bool compare(const ScanNode& lhs, const ScanNode& rhs) const {
        return lhs.getScanDefName() == rhs.getScanDefName();
    }

    bool compare(const FilterNode& lhs, const FilterNode& rhs) const {
        return lhs.getFilter() == rhs.getFilter() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const EvaluationNode& lhs, const EvaluationNode& rhs) const {
        return lhs.binder() == rhs.binder() && lhs.getProjection() == rhs.getProjection() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const SargableNode& lhs, const SargableNode& rhs) const {
        return lhs.getReqMap() == rhs.getReqMap() &&
            lhs.getCandidateIndexMap() == rhs.getCandidateIndexMap() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const RIDIntersectNode& lhs, const RIDIntersectNode& rhs) const {
        // Specifically always including children.
        return lhs.getScanProjectionName() == rhs.getScanProjectionName() &&
            lhs.getLeftChild() == rhs.getLeftChild() && lhs.getRightChild() == rhs.getRightChild();
    }

    bool compare(const MemoLogicalDelegatorNode& lhs, const MemoLogicalDelegatorNode& rhs) const {
        return lhs.getGroupId() == rhs.getGroupId();
    }

    bool compare(const BinaryJoinNode& lhs, const BinaryJoinNode& rhs) const {
        // Specifically always including children.
        return lhs.getJoinType() == rhs.getJoinType() && lhs.getLeftChild() == rhs.getLeftChild() &&
            lhs.getRightChild() == rhs.getRightChild();
    }

    bool compare(const InnerMultiJoinNode& lhs, const InnerMultiJoinNode& rhs) const {
        // Specifically always including children.
        return lhs.nodes() == rhs.nodes();
    }

    bool compare(const UnionNode& lhs, const UnionNode& rhs) const {
        // Specifically always including children.
        return lhs.binder() == rhs.binder() && lhs.nodes() == rhs.nodes();
    }

    bool compare(const GroupByNode& lhs, const GroupByNode& rhs) const {
        return lhs.getAggregationProjectionNames() == rhs.getAggregationProjectionNames() &&
            lhs.getAggregationProjections() == rhs.getAggregationProjections() &&
            lhs.getGroupByProjectionNames() == rhs.getGroupByProjectionNames() &&
            lhs.isLocal() == rhs.isLocal() &&
            lhs.canRewriteIntoLocal() == rhs.canRewriteIntoLocal() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const UnwindNode& lhs, const UnwindNode& rhs) const {
        return lhs.binder() == rhs.binder() &&
            lhs.getRetainNonArrays() == rhs.getRetainNonArrays() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const WindNode& lhs, const WindNode& rhs) const {
        return lhs.binder() == rhs.binder() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const CollationNode& lhs, const CollationNode& rhs) const {
        return lhs.getProperty() == rhs.getProperty() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const LimitSkipNode& lhs, const LimitSkipNode& rhs) const {
        return lhs.getProperty() == rhs.getProperty() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const ExchangeNode& lhs, const ExchangeNode& rhs) const {
        return lhs.getProperty() == rhs.getProperty() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    bool compare(const RootNode& lhs, const RootNode& rhs) const {
        return lhs.getProperty() == rhs.getProperty() &&
            (!includeChildNodes || lhs.getChild() == rhs.getChild());
    }

    template <typename T>
    bool compare(const T&, const T&) const {
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its compare.");
        uasserted(0, "must implement custom compare");
    }

    template <typename T, typename T1, typename... Ts>
    bool walk(const T& lhs, const T1& rhs, Ts&&...) const {
        invariant(rhs.template is<T>());
        return compare(lhs, *rhs.template cast<T>());
    }

    template <typename T>
    bool compareABT(const T& lhs, const T& rhs) const {
        if (lhs.tagOf() != rhs.tagOf()) {
            return false;
        }
        return algebra::walk<false>(lhs, *this, rhs);
    }
};

bool ABTCompare::compareIncludeChildren(const ABT& left, const ABT& right) {
    ABTCompareTransporter<true> comparator;
    return comparator.compareABT<ABT>(left, right);
}

bool ABTCompare::compareIncludeChildrenRef(const ABT::reference_type& left,
                                           const ABT::reference_type& right) {
    ABTCompareTransporter<true> comparator;
    return comparator.compareABT<ABT::reference_type>(left, right);
}

bool ABTCompare::compareNoChildren(const ABT& left, const ABT& right) {
    ABTCompareTransporter<false> comparator;
    return comparator.compareABT<ABT>(left, right);
}

bool ABTCompare::compareNoChildrenRef(const ABT::reference_type& left,
                                      const ABT::reference_type& right) {
    ABTCompareTransporter<false> comparator;
    return comparator.compareABT<ABT::reference_type>(left, right);
}

}  // namespace mongo::optimizer
