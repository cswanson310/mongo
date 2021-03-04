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

#include "mongo/db/query/optimizer/cascades/cost_derivation.h"

namespace mongo::optimizer::cascades {

using namespace properties;

class CostDerivation {
    // TODO: calibration?
    static constexpr double kScanInitialCost = 100000.0;
    static constexpr double kScanIncrementalCost = 1000.0;

    static constexpr double kFilterIncrementalCost = 1000.0;
    static constexpr double kEvalIncrementalCost = 1000.0;
    static constexpr double kGroupByIncrementalCost = 10000.0;
    static constexpr double kUnwindIncrementalCost = 10000;

    // Nlog(N)?
    static constexpr double kCollationIncrementalCost = 100000;

    static constexpr double kExchangeIncrementalCost = 100;
    static constexpr double kExchangePartitioningCost = 100;

public:
    double operator()(const ABT& /*n*/, const PhysicalScanNode& node) {
        // Default estimate for scan.
        return kScanInitialCost + kScanIncrementalCost * _cardinalityEstimate;
    }

    double operator()(const ABT& /*n*/, const CoScanNode& node) {
        // Assumed to be free.
        return 0.0;
    }

    double operator()(const ABT& /*n*/, const IndexScanNode& node) {
        // Assume index scan is cheaper than regular scan.
        return kScanInitialCost + kScanIncrementalCost * _cardinalityEstimate / 2;
    }

    double operator()(const ABT& /*n*/, const SeekNode& node) {
        // For now assume SeekNode is free.
        return 0.0;
    }

    double operator()(const ABT& /*n*/, const MemoPhysicalDelegatorNode& node) {
        // Do not include cost of children.
        return 0.0;
    }

    double operator()(const ABT& /*n*/, const FilterNode& node) {
        if (node.getFilter() == Constant::boolean(true)) {
            // Trivially true filters are generated during index optimization.
            return 0.0;
        }

        const CEType childCE = getChildCE(node);
        return kFilterIncrementalCost * childCE;
    }

    double operator()(const ABT& /*n*/, const EvaluationNode& node) {
        const CEType childCE = getChildCE(node);
        return kEvalIncrementalCost * childCE;
    }

    double operator()(const ABT& /*n*/, const InnerMultiJoinNode& node) {
        uasserted(6624000, "Cost derivation not implemented.");
    }

    double operator()(const ABT& /*n*/, const BinaryJoinNode& node) {
        if (node.getFilter() == Constant::boolean(true) &&
            !node.getLeftChild().is<MemoLogicalDelegatorNode>()) {
            // Assume for now this node is generated during index optimization since it has a left
            // physical child.
            return 0.0;
        }

        // TODO: handle general case.
        // TODO: handle indexing requirements.
        return 0.0;
    }

    double operator()(const ABT& /*n*/, const HashJoinNode& node) {
        // TODO: implement costing.

        return 0.0;
    }

    double operator()(const ABT& /*n*/, const UnionNode& node) {
        uasserted(6624000, "Cost derivation not implemented.");
    }

    double operator()(const ABT& /*n*/, const GroupByNode& node) {
        const CEType childCE = getChildCE(node);
        // TODO: consider RepetitionEstimate since this is a stateful operation.
        return kGroupByIncrementalCost * childCE;
    }

    double operator()(const ABT& /*n*/, const UnwindNode& node) {
        // Unwind probably depends mostly on its output size.
        return kUnwindIncrementalCost * _cardinalityEstimate;
    }

    double operator()(const ABT& /*n*/, const WindNode& node) {
        uasserted(6624000, "Cost derivation not implemented.");
    }

    double operator()(const ABT& /*n*/, const CollationNode& node) {
        // TODO: consider RepetitionEstimate since this is a stateful operation.
        return kCollationIncrementalCost * _cardinalityEstimate;
    }

    double operator()(const ABT& /*n*/, const LimitSkipNode& node) {
        return kScanIncrementalCost * _cardinalityEstimate;
    }

    double operator()(const ABT& /*n*/, const ExchangeNode& node) {
        double result = kExchangeIncrementalCost * _cardinalityEstimate;

        switch (node.getProperty().getType()) {
            case DistributionType::Replicated:
                result *= _metadata._numberOfPartitions;
                break;

            case DistributionType::HashPartitioning:
            case DistributionType::RangePartitioning:
                result *= kExchangePartitioningCost;
                break;

            default:
                break;
        }

        return result;
    }

    double operator()(const ABT& /*n*/, const RootNode& node) {
        return 0.0;
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    double operator()(const ABT& /*n*/, const T& /*node*/, Ts&&...) {
        static_assert(!canBePhysicalNode<T>(),
                      "Physical node must implement its logical property derivation.");
        return 0.0;
    }

    static CostType derive(const Metadata& metadata,
                           Memo& memo,
                           const Properties& logicalProps,
                           const Properties& physProps,
                           const ABT::reference_type physNodeRef) {
        CostDerivation instance(metadata, memo, logicalProps, physProps);
        double result = physNodeRef.visit(instance);

        switch (getPropertyConst<DistributionRequirement>(physProps).getType()) {
            case DistributionType::Centralized:
            case DistributionType::Replicated:
                break;

            case DistributionType::RoundRobin:
            case DistributionType::HashPartitioning:
            case DistributionType::RangePartitioning:
            case DistributionType::UnknownPartitioning:
                result /= metadata._numberOfPartitions;
                break;

            default:
                MONGO_UNREACHABLE;
        }

        return CostType::fromDouble(result);
    }

private:
    CostDerivation(const Metadata& metadata,
                   Memo& memo,
                   const Properties& logicalProps,
                   const Properties& physProps)
        : _metadata(metadata),
          _memo(memo),
          _logicalProps(logicalProps),
          _physProps(physProps),
          _cardinalityEstimate(getCardinalityEstimate(_logicalProps, _physProps)) {}

    template <class T>
    CEType getChildCE(const T& n) {
        const GroupIdType childGroupId =
            n.getChild().template cast<MemoLogicalDelegatorNode>()->getGroupId();
        const auto& childLogicalProps = _memo.getGroup(childGroupId)._logicalProperties;

        // Here we assume that the limit-skip property will also apply to child.
        return getCardinalityEstimate(childLogicalProps, _physProps);
    }

    static CEType getCardinalityEstimate(const Properties& logicalProps,
                                         const Properties& physProps) {
        CEType result = getPropertyConst<CardinalityEstimate>(logicalProps).getEstimate();
        if (hasProperty<LimitSkipRequirement>(physProps)) {
            auto limit = getPropertyConst<LimitSkipRequirement>(physProps).getAbsoluteLimit();
            if (result > limit) {
                result = limit;
            }
        }

        if (hasProperty<RepetitionEstimate>(physProps)) {
            result *= getPropertyConst<RepetitionEstimate>(physProps).getEstimate();
        }

        return result;
    }

    // We don't own this.
    const Metadata& _metadata;
    Memo& _memo;
    const Properties& _logicalProps;
    const Properties& _physProps;
    const CEType _cardinalityEstimate;
};

CostType deriveCost(const Metadata& metadata,
                    Memo& memo,
                    const Properties& logicalProps,
                    const Properties& physProps,
                    const ABT::reference_type physNodeRef) {
    return CostDerivation::derive(metadata, memo, logicalProps, physProps, physNodeRef);
}

}  // namespace mongo::optimizer::cascades
