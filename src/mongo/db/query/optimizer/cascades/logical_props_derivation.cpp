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

#include "mongo/db/query/optimizer/cascades/logical_props_derivation.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

using namespace properties;

class DeriveLogicalProperties {
public:
    Properties transport(const ScanNode& node, Properties /*bindResult*/) {
        DistributionSet distributions;
        std::vector<ProjectionNameOrderPreservingSet> sargableProjectionNames;
        {
            // Populate initial distributions for node.
            const auto& distributionAndPaths =
                _metadata._scanDefs.at(node.getScanDefName()).getDistributionAndPaths();
            switch (distributionAndPaths._type) {
                case DistributionType::Centralized:
                    distributions.insert({DistributionType::Centralized});
                    break;

                case DistributionType::Replicated:
                    uassert(
                        0, "Invalid distribution specification", _metadata._numberOfPartitions > 1);

                    distributions.insert({DistributionType::Centralized});
                    distributions.insert({DistributionType::Replicated});
                    break;

                case DistributionType::HashPartitioning:
                case DistributionType::RangePartitioning:
                    uassert(
                        0, "Invalid distribution specification", _metadata._numberOfPartitions > 1);

                    distributions.insert({DistributionType::UnknownPartitioning});
                    // Allocate entries for each of the partition paths.
                    sargableProjectionNames.resize(distributionAndPaths._paths.size());
                    break;

                case DistributionType::UnknownPartitioning:
                    uassert(
                        0, "Invalid distribution specification", _metadata._numberOfPartitions > 1);

                    distributions.insert({DistributionType::UnknownPartitioning});
                    break;

                default:
                    uasserted(0, "Invalid collection distribution");
            }

            // TODO: populate distributions for associated indexes.
        }

        return makeProperties(IndexingAvailability(_groupId,
                                                   node.getProjectionName(),
                                                   std::move(sargableProjectionNames)),
                              CollectionAvailability({node.getScanDefName()}),
                              DistributionAvailability(std::move(distributions)));
    }

    Properties transport(const MemoLogicalDelegatorNode& node) {
        return _memo.getGroup(node.getGroupId())._logicalProperties;
    }

    Properties transport(const FilterNode& node,
                         Properties childResult,
                         Properties /*exprResult*/) {
        // Propagate indexing, collection, and distribution availabilities.
        Properties result = std::move(childResult);
        addCentralizedAndReplicatedDistributions(result);
        return result;
    }

    class FieldPathExtractor {
    public:
        void transport(const PathGet& node) {
            _path.push_back(node.name());
        }

        template <typename T, typename... Ts>
        void transport(const T& /*node*/, Ts&&...) {
            // noop
        }

        static FieldPathType extract(const ABT& node) {
            FieldPathExtractor instance;
            algebra::transport<false>(node, instance);
            return instance._path;
        }

    private:
        FieldPathType _path;
    };

    static bool possibleFieldPathMatch(const FieldPathType& nodePath,
                                       const FieldPathType& collectionPath) {
        if (nodePath.size() > collectionPath.size()) {
            return false;
        }

        // Naive search to discover if nodePath appears within collectionPath.
        for (size_t i = 0; i < collectionPath.size() - nodePath.size(); i++) {
            bool match = true;
            for (size_t j = 0; j < nodePath.size(); j++) {
                if (nodePath.at(j) != collectionPath.at(i + j)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }

        return false;
    }

    Properties transport(const EvaluationNode& node,
                         Properties childResult,
                         Properties /*exprResult*/) {
        // We are specifically not adding the node's projection to ProjectionAvailability here.
        // The logical properties already contains projection availability which is derived first
        // when the memo group is created.

        Properties result = std::move(childResult);
        auto& distributions = getProperty<DistributionAvailability>(result).getDistributionSet();
        addCentralizedAndReplicatedDistributions(distributions);

        if (!hasProperty<IndexingAvailability>(result)) {
            return result;
        }
        const PartialSchemaReqConversion& conversion =
            convertExprToPartialSchemaReq(node.getProjection());
        if (!conversion._success) {
            return result;
        }
        // A sargable node can be extracted.

        auto& sargableProjectionNames =
            getProperty<IndexingAvailability>(result).getSargableProjectionNames();
        const auto& scanDef = _metadata._scanDefs.at(
            *getPropertyConst<CollectionAvailability>(result).getScanDefSet().begin());
        const auto& distributionAndPaths = scanDef.getDistributionAndPaths();
        uassert(
            0,
            "Invalid number of entries in Indexing availability sargable projection names vector",
            sargableProjectionNames.size() == distributionAndPaths._paths.size());

        for (const auto& entry : conversion._reqMap) {
            const FieldPathType& nodePath = FieldPathExtractor::extract(entry.first._path);
            for (size_t i = 0; i < sargableProjectionNames.size(); i++) {
                auto& sargableProjectionName = sargableProjectionNames.at(i);
                const FieldPathType& collectionPath =
                    FieldPathExtractor::extract(distributionAndPaths._paths.at(i));
                if (possibleFieldPathMatch(nodePath, collectionPath)) {
                    sargableProjectionName.emplace_back(node.getProjectionName());
                }

                if (sargableProjectionName.getVector().empty()) {
                    // Do not generate distributions if we do not have candidate projections for
                    // each partitioning path.
                    return result;
                }
            }
        }

        switch (distributionAndPaths._type) {
            case DistributionType::HashPartitioning:
            case DistributionType::RangePartitioning: {
                size_t numberOfAlternatives = 1;

                // Pick one of the alternative projection for each position.
                for (size_t altIndex = 0; altIndex < numberOfAlternatives; altIndex++) {
                    ProjectionNameVector projections;

                    size_t remainder = altIndex;
                    for (const auto& entry : sargableProjectionNames) {
                        const size_t size = entry.getVector().size();
                        uassert(0, "Size must be positive", size > 0);

                        projections.push_back(entry.getVector().at(remainder % size));
                        remainder /= size;
                    }
                    uassert(0, "Remainder must be zero at this point", remainder == 0);

                    distributions.emplace(distributionAndPaths._type, std::move(projections));
                }
            }

            default:
                break;
        }

        return result;
    }

    Properties transport(const SargableNode& node,
                         Properties /*childResult*/,
                         Properties /*bindsResult*/,
                         Properties /*refsResult*/) {
        // Properties for the group should already be derived via the underlying Filter or
        // Evaluation logical nodes.
        uasserted(6624000, "Should not be necessary to derive properties for SargableNode");
    }

    Properties transport(const RIDIntersectNode& node,
                         Properties /*leftChildResult*/,
                         Properties /*rightChildResult*/) {
        // Properties for the group should already be derived via the underlying Filter or
        // Evaluation logical nodes.
        uasserted(6624000, "Should not be necessary to derive properties for RIDIntersectNode");
    }

    Properties transport(const InnerMultiJoinNode& node, std::vector<Properties> /*childResults*/) {
        // TODO: remove indexing potential property when implemented.
        // TODO: combine scan defs from all children for CollectionAvailability.
        uasserted(6624000, "Logical property derivation not implemented.");
    }

    Properties transport(const BinaryJoinNode& node,
                         Properties /*leftChildResult*/,
                         Properties /*rightChildResult*/,
                         Properties /*exprResult*/) {
        // TODO: remove indexing potential property when implemented.
        // TODO: combine scan defs from all children for CollectionAvailability.
        uasserted(6624000, "Logical property derivation not implemented.");
    }

    Properties transport(const UnionNode& node,
                         std::vector<Properties> /*childResults*/,
                         Properties /*bindResult*/,
                         Properties /*refsResult*/) {
        // TODO: remove indexing potential property when implemented.
        // TODO: combine scan defs from all children for CollectionAvailability.
        uasserted(6624000, "Logical property derivation not implemented.");
    }

    Properties transport(const GroupByNode& node,
                         Properties childResult,
                         Properties /*bindAggResult*/,
                         Properties /*refsAggResult*/,
                         Properties /*bindGbResult*/,
                         Properties /*refsGbResult*/) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);

        auto& distributions = getProperty<DistributionAvailability>(result).getDistributionSet();
        addCentralizedAndReplicatedDistributions(distributions);

        if (_metadata._numberOfPartitions > 1 && !node.isLocal()) {
            distributions.erase({DistributionType::UnknownPartitioning});
            distributions.erase({DistributionType::RoundRobin});

            // We propagate hash and range partitioning only if we are global agg.
            const ProjectionNameVector& groupByProjections = node.getGroupByProjectionNames();
            if (!groupByProjections.empty()) {
                DistributionRequirement allowedRangePartitioning{
                    DistributionType::RangePartitioning, groupByProjections};
                for (auto it = distributions.begin(); it != distributions.end();) {
                    switch (it->getType()) {
                        case DistributionType::HashPartitioning:
                            // Erase all hash partition distributions. New ones will be generated
                            // after.
                            it = distributions.erase(it);
                            break;

                        case DistributionType::RangePartitioning:
                            // Retain only the range partition which contains the group by
                            // projections in the node order.
                            if (*it == allowedRangePartitioning) {
                                it++;
                            } else {
                                it = distributions.erase(it);
                            }

                        default:
                            it++;
                            break;
                    }
                }

                // Generate hash distributions using the power set of group-by projections.
                for (size_t mask = 1; mask < (1ull << groupByProjections.size()); mask++) {
                    ProjectionNameVector projectionNames;
                    for (size_t index = 0; index < groupByProjections.size(); index++) {
                        if ((mask & (1ull << index)) != 0) {
                            projectionNames.push_back(groupByProjections.at(index));
                        }
                    }
                    distributions.emplace(DistributionType::HashPartitioning,
                                          std::move(projectionNames));
                }
            }
        }

        return result;
    }

    Properties transport(const UnwindNode& node,
                         Properties childResult,
                         Properties /*bindResult*/,
                         Properties /*refsResult*/) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);

        const ProjectionName& unwoundProjectionName = node.getProjectionName();
        auto& distributions = getProperty<DistributionAvailability>(result).getDistributionSet();
        addCentralizedAndReplicatedDistributions(distributions);

        if (_metadata._numberOfPartitions > 1) {
            for (auto it = distributions.begin(); it != distributions.end();) {
                switch (it->getType()) {
                    case DistributionType::HashPartitioning:
                    case DistributionType::RangePartitioning: {
                        // Erase partitioned distributions which contain the projection to unwind.
                        bool containsProjection = false;
                        for (const ProjectionName& projectionName : it->getProjections()) {
                            if (projectionName == unwoundProjectionName) {
                                containsProjection = true;
                                break;
                            }
                        }
                        if (containsProjection) {
                            it = distributions.erase(it);
                        } else {
                            it++;
                        }
                        break;
                    }

                    default:
                        it++;
                        break;
                }
            }
        }

        return result;
    }

    Properties transport(const WindNode& node,
                         Properties /*childResult*/,
                         Properties /*bindResult*/,
                         Properties /*refsResult*/) {
        // TODO: remove indexing potential property when implemented.
        uasserted(6624000, "Logical property derivation not implemented.");
    }

    Properties transport(const CollationNode& node,
                         Properties childResult,
                         Properties /*refsResult*/) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);
        addCentralizedAndReplicatedDistributions(result);
        return result;
    }

    Properties transport(const LimitSkipNode& node, Properties childResult) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);
        addCentralizedAndReplicatedDistributions(result);
        return result;
    }

    Properties transport(const ExchangeNode& node,
                         Properties childResult,
                         Properties /*refsResult*/) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);
        addCentralizedAndReplicatedDistributions(result);
        return result;
    }

    Properties transport(const RootNode& node, Properties childResult, Properties /*refsResult*/) {
        Properties result = std::move(childResult);
        removeProperty<IndexingAvailability>(result);
        addCentralizedAndReplicatedDistributions(result);
        return result;
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    Properties transport(const T& /*node*/, Ts&&...) {
        static_assert(!canBeLogicalNode<T>(),
                      "Logical node must implement its logical property derivation.");
        return {};
    }

    static Properties derive(const Memo& memo,
                             const Metadata& metadata,
                             const GroupIdType groupId) {
        DeriveLogicalProperties instance(memo, metadata, groupId);
        const ABT::reference_type nodeRef = memo.getGroup(groupId)._logicalNodes.at(0);
        return algebra::transport<false>(nodeRef, instance);
    }

private:
    DeriveLogicalProperties(const Memo& memo, const Metadata& metadata, const GroupIdType groupId)
        : _groupId(groupId), _memo(memo), _metadata(metadata) {}

    void addCentralizedAndReplicatedDistributions(DistributionSet& distributions) {
        distributions.emplace(DistributionType::Centralized);
        if (_metadata._numberOfPartitions > 1) {
            distributions.emplace(DistributionType::Replicated);
        }
    }

    void addCentralizedAndReplicatedDistributions(Properties& properties) {
        addCentralizedAndReplicatedDistributions(
            getProperty<DistributionAvailability>(properties).getDistributionSet());
    }

    const GroupIdType _groupId;

    // We don't own any of those.
    const Memo& _memo;
    const Metadata& _metadata;
};

Properties logicalPropsDerive(const Memo& memo,
                              const Metadata& metadata,
                              const GroupIdType groupId) {
    return DeriveLogicalProperties::derive(memo, metadata, groupId);
}

}  // namespace mongo::optimizer::cascades
