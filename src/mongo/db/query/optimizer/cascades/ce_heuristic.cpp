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

#include "mongo/db/query/optimizer/cascades/ce_heuristic.h"

namespace mongo::optimizer::cascades {

using namespace properties;

class CEHeuristicTransport {
public:
    CEType transport(const ScanNode& node, CEType /*bindResult*/) {
        // Default cardinality estimate.
        return 1000.00;
    }

    CEType transport(const MemoLogicalDelegatorNode& node) {
        return getPropertyConst<CardinalityEstimate>(
                   _memo.getGroup(node.getGroupId())._logicalProperties)
            .getEstimate();
    }

    CEType transport(const FilterNode& node, CEType childResult, CEType /*exprResult*/) {
        if (node.getFilter() == Constant::boolean(true)) {
            // Trivially true filter.
            return childResult;
        } else if (node.getFilter() == Constant::boolean(false)) {
            // Trivially false filter.
            return 0.0;
        } else {
            // Estimate filter selectivity at 0.5.
            return 0.5 * childResult;
        }
    }

    CEType transport(const EvaluationNode& node, CEType childResult, CEType /*exprResult*/) {
        // Evaluations do not change cardinality.
        return childResult;
    }

    CEType transport(const SargableNode& node,
                     CEType /*childResult*/,
                     CEType /*bindsResult*/,
                     CEType /*refsResult*/) {
        // Properties for the group should already be derived via the underlying Filter or
        // Evaluation logical nodes.
        uasserted(6624000, "Should not be necessary to derive properties for SargableNode");
    }

    CEType transport(const RIDIntersectNode& node,
                     CEType /*leftChildResult*/,
                     CEType /*rightChildResult*/) {
        // Properties for the group should already be derived via the underlying Filter or
        // Evaluation logical nodes.
        uasserted(6624000, "Should not be necessary to derive properties for RIDIntersectNode");
    }

    CEType transport(const InnerMultiJoinNode& node, std::vector<CEType> /*childResults*/) {
        uasserted(6624000, "CE derivation not implemented.");
    }

    CEType transport(const BinaryJoinNode& node,
                     CEType /*leftChildResult*/,
                     CEType /*rightChildResult*/,
                     CEType /*exprResult*/) {
        uasserted(6624000, "CE derivation not implemented.");
    }

    CEType transport(const UnionNode& node,
                     std::vector<CEType> /*childResults*/,
                     CEType /*bindResult*/,
                     CEType /*refsResult*/) {
        uasserted(6624000, "CE derivation not implemented.");
    }

    CEType transport(const GroupByNode& node,
                     CEType childResult,
                     CEType /*bindAggResult*/,
                     CEType /*refsAggResult*/,
                     CEType /*bindGbResult*/,
                     CEType /*refsGbResult*/) {
        // TODO: estimate number of groups.
        if (node.canRewriteIntoLocal()) {
            return 0.01 * childResult;
        } else {
            return 0.1 * childResult;
        }
    }

    CEType transport(const UnwindNode& node,
                     CEType childResult,
                     CEType /*bindResult*/,
                     CEType /*refsResult*/) {
        // Estimate unwind selectivity at 2.0
        return 2.0 * childResult;
    }

    CEType transport(const WindNode& node,
                     CEType /*childResult*/,
                     CEType /*bindResult*/,
                     CEType /*refsResult*/) {
        uasserted(6624000, "CE derivation not implemented.");
    }

    CEType transport(const CollationNode& node, CEType childResult, CEType /*refsResult*/) {
        // Collations do not change cardinality.
        return childResult;
    }

    CEType transport(const LimitSkipNode& node, CEType childResult) {
        const auto limit = node.getProperty().getLimit();
        if (limit < childResult) {
            return limit;
        }
        return childResult;
    }

    CEType transport(const ExchangeNode& node, CEType childResult, CEType /*refsResult*/) {
        // Exchanges do not change cardinality.
        return childResult;
    }

    CEType transport(const RootNode& node, CEType childResult, CEType /*refsResult*/) {
        // Root node does not change cardinality.
        return childResult;
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    CEType transport(const T& /*node*/, Ts&&...) {
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its CE derivation.");
        return 0.0;
    }

    static CEType derive(const Memo& memo, const GroupIdType groupId) {
        CEHeuristicTransport instance(memo);
        const ABT::reference_type nodeRef = memo.getGroup(groupId)._logicalNodes.at(0);
        return algebra::transport<false>(nodeRef, instance);
    }

private:
    CEHeuristicTransport(const Memo& memo) : _memo(memo) {}

    // We don't own this.
    const Memo& _memo;
};

CEType heuristicCE(const Memo& memo, const GroupIdType groupId) {
    return CEHeuristicTransport::derive(memo, groupId);
}

}  // namespace mongo::optimizer::cascades
