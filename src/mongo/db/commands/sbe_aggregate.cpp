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

#include "mongo/db/commands/sbe_aggregate.h"

#include "mongo/db/exec/sbe/abt/abt_lower.h"
#include "mongo/db/pipeline/abt/abt_document_source_visitor.h"
#include "mongo/db/pipeline/abt/match_expression_visitor.h"
#include "mongo/db/query/optimizer/cascades/ce_heuristic.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/db/query/sbe_stage_builder.h"

namespace mongo {

using namespace optimizer;

class CESamplingTransport {
public:
    CESamplingTransport(OperationContext* opCtx,
                        const CEType collectionCE,
                        OptPhaseManager& phaseManager)
        : _evalPlan(make<Blackhole>()),
          _collectionCE(collectionCE),
          _opCtx(opCtx),
          _phaseManager(phaseManager) {}

    CEType transport(const ABT& n,
                     const ScanNode& /*node*/,
                     const Memo& /*memo*/,
                     const GroupIdType /*groupId*/,
                     const properties::Properties& /*logicalProps*/,
                     CEType /*bindResult*/) {
        // We will lower the scan node in a sampling context here.
        // TODO: for now just return the documents in random order.
        _evalPlan = make<LimitSkipNode>(properties::LimitSkipRequirement(_collectionCE, 0), n);
        return _collectionCE;
    }

    CEType transport(const ABT& /*n*/,
                     const MemoLogicalDelegatorNode& node,
                     const Memo& memo,
                     const GroupIdType /*groupId*/,
                     const properties::Properties& /*logicalProps*/) {
        return properties::getPropertyConst<properties::CardinalityEstimate>(
                   memo.getGroup(node.getGroupId())._logicalProperties)
            .getEstimate();
    }

    CEType transport(const ABT& /*n*/,
                     const FilterNode& node,
                     const Memo& memo,
                     const GroupIdType groupId,
                     const properties::Properties& logicalProps,
                     CEType childResult,
                     CEType /*exprResult*/) {
        if (!properties::hasProperty<properties::IndexingAvailability>(logicalProps)) {
            return heuristicCE(memo, groupId);
        }

        const auto [success, selectivity] = estimateFilterSelectivity(node);
        if (!success) {
            return heuristicCE(memo, groupId);
        }
        return selectivity * childResult;
    }

    CEType transport(const ABT& n,
                     const EvaluationNode& /*node*/,
                     const Memo& memo,
                     const GroupIdType groupId,
                     const properties::Properties& logicalProps,
                     CEType childResult,
                     CEType /*exprResult*/) {
        if (!properties::hasProperty<properties::IndexingAvailability>(logicalProps)) {
            return heuristicCE(memo, groupId);
        }

        // Append node to evalPlan.
        ABT temp = n;
        std::swap(temp.cast<EvaluationNode>()->getChild(), _evalPlan);
        std::swap(temp, _evalPlan);

        // Evaluations do not change cardinality.
        return childResult;
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    CEType transport(const ABT& /*n*/,
                     const T& /*node*/,
                     const Memo& memo,
                     const GroupIdType groupId,
                     const properties::Properties& /*logicalProps*/,
                     Ts&&...) {
        if (canBeLogicalNode<T>()) {
            return heuristicCE(memo, groupId);
        }
        return 0.0;
    }

    CEType derive(const Memo& memo, const GroupIdType groupId) {
        const Group& group = memo.getGroup(groupId);
        auto nodeRef = group._logicalNodes.at(0);
        return algebra::transport<true>(nodeRef, *this, memo, groupId, group._logicalProperties);
    }

private:
    std::pair<bool, SelectivityType> estimateFilterSelectivity(const FilterNode& node) {
        // Create a plan with all eval nodes so far and the filter last.
        ABT abtTree = make<FilterNode>(node.getFilter(), _evalPlan);

        // Add a group by to count number of documents.
        const ProjectionName sampleSumProjection = "sum";
        abtTree =
            make<GroupByNode>(ProjectionNameVector{},
                              ProjectionNameVector{sampleSumProjection},
                              ABTVector{make<FunctionCall>("$sum", ABTVector{Constant::int64(1)})},
                              std::move(abtTree));
        abtTree = make<RootNode>(
            properties::ProjectionRequirement{ProjectionNameVector{sampleSumProjection}},
            std::move(abtTree));

        if (!_phaseManager.optimize(abtTree)) {
            return {false, {}};
        }

        std::cerr << "********* Sampling ABT *********\n";
        std::cerr << ExplainGenerator::explainV2(abtTree);
        std::cerr << "********* Sampling ABT *********\n";

        auto env = VariableEnvironment::build(abtTree);
        SlotVarMap slotMap;
        sbe::value::SlotIdGenerator ids;
        SBENodeLowering g{env, slotMap, ids, _phaseManager, true /*randomScan*/};
        auto sbePlan = g.optimize(abtTree);

        // TODO: return errors on those?
        uassert(0, "Lowering failed", sbePlan != nullptr);
        uassert(0, "Invalid slot map size", slotMap.size() == 1);

        sbe::CompileCtx ctx(std::make_unique<sbe::RuntimeEnvironment>());
        sbePlan->prepare(ctx);

        std::vector<sbe::value::SlotAccessor*> accessors;
        for (auto& [name, slot] : slotMap) {
            accessors.emplace_back(sbePlan->getAccessor(ctx, slot));
        }

        sbePlan->attachToOperationContext(_opCtx);
        sbePlan->open(false);
        ON_BLOCK_EXIT([&] { sbePlan->close(); });

        while (sbePlan->getNext() != sbe::PlanState::IS_EOF) {
            const auto [tag, value] = accessors.at(0)->getViewOfValue();
            if (tag == sbe::value::TypeTags::NumberInt64) {
                // TODO: check if we get exactly one result from the groupby?
                return {true, value / _collectionCE};
            }
            return {false, {}};
        };

        return {false, {}};
    }

    ABT _evalPlan;
    const CEType _collectionCE;

    // We don't own those.
    OperationContext* _opCtx;
    OptPhaseManager& _phaseManager;
};

static std::unordered_map<std::string, optimizer::IndexDefinition> buildIndexSpecsOptimizer(
    boost::intrusive_ptr<ExpressionContext> expCtx,
    OperationContext* opCtx,
    const IndexCatalog& indexCatalog,
    const optimizer::ProjectionName& scanProjName) {
    using namespace optimizer;

    std::unordered_map<std::string, IndexDefinition> result;
    auto indexIterator = indexCatalog.getIndexIterator(opCtx, false /*includeUnfinished*/);

    while (indexIterator->more()) {
        const IndexCatalogEntry& catalogEntry = *indexIterator->next();
        const IndexDescriptor& descriptor = *catalogEntry.descriptor();
        if (descriptor.hidden() || descriptor.isSparse() ||
            descriptor.getIndexType() != IndexType::INDEX_BTREE) {
            // Not supported for now.
            continue;
        }

        // SBE version is base 0.
        const int64_t version = static_cast<int>(descriptor.version()) - 1;

        uint32_t orderingBits = 0;
        {
            const Ordering ordering = catalogEntry.ordering();
            for (int i = 0; i < descriptor.getNumFields(); i++) {
                if ((ordering.get(i) == 1)) {
                    orderingBits |= (1ull << i);
                }
            }
        }

        IndexCollationSpec indexCollationSpec;
        BSONObjIterator it(descriptor.keyPattern());
        while (it.more()) {
            auto element = it.next();

            bool useIndex = true;
            FieldPathType fieldPath;
            FieldPath path(element.fieldName());
            for (size_t i = 0; i < path.getPathLength(); i++) {
                std::string fieldName = path.getFieldName(i).toString();
                if (fieldName == "$**") {
                    // TODO: For now disallow wildcard indexes.
                    useIndex = false;
                    break;
                }
                fieldPath.emplace_back(path.getFieldName(i).toString());
            }
            if (!useIndex) {
                continue;
            }

            const int value = element.numberInt();
            if (value != -1 && value != 1) {
                // Invalid value?
                continue;
            }

            const CollationOp collationOp =
                (value == 1) ? CollationOp::Ascending : CollationOp::Descending;
            indexCollationSpec.emplace_back(std::move(fieldPath), collationOp);
        }

        PartialSchemaRequirements partialIndexReqMap;
        if (descriptor.isPartial()) {
            auto expr = MatchExpressionParser::parseAndNormalize(
                descriptor.partialFilterExpression(),
                expCtx,
                ExtensionsCallbackNoop(),
                MatchExpressionParser::kBanAllSpecialFeatures);

            ABT exprABT = generateMatchExpression(expr.get(), false /*allowAggExpression*/, "", "");
            exprABT = make<EvalFilter>(std::move(exprABT), make<Variable>(scanProjName));

            // TODO: simplify expression.

            PartialSchemaReqConversion conversion = convertExprToPartialSchemaReq(exprABT);
            if (!conversion._success) {
                continue;
            }
            partialIndexReqMap = std::move(conversion._reqMap);

            // Insert entries with fully open intervals for regular index fields (no bounds
            // restrictions on them). Do not overwrite existing entries from the partial index
            // filter.
            for (const auto& collationEntry : indexCollationSpec) {
                partialIndexReqMap.emplace(PartialSchemaKey{scanProjName, collationEntry._path},
                                           PartialSchemaRequirement{});
            }
        }

        // For now we assume distribution is Centralized.
        result.emplace(descriptor.indexName(),
                       IndexDefinition(std::move(indexCollationSpec),
                                       version,
                                       orderingBits,
                                       DistributionType::Centralized,
                                       std::move(partialIndexReqMap)));
    }

    return result;
}

std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> optimizeAndCreateExecutor(
    OptPhaseManager& phaseManager,
    ABT abtTree,
    OperationContext* opCtx,
    const NamespaceString& nss,
    const CollectionPtr& collection) {
    uassert(0, "Optimization failed", phaseManager.optimize(abtTree));

    std::cerr << "********* Optimized ABT *********\n";
    std::cerr << ExplainGenerator::explainV2(
        make<MemoPhysicalDelegatorNode>(phaseManager.getPhysicalNodeId()),
        true /*displayPhysicalProperties*/,
        &phaseManager.getMemo());
    std::cerr << "********* Optimized ABT *********\n";

    auto env = VariableEnvironment::build(abtTree);
    SlotVarMap slotMap;
    sbe::value::SlotIdGenerator ids;
    SBENodeLowering g{env, slotMap, ids, phaseManager};
    auto sbePlan = g.optimize(abtTree);

    uassert(0, "Lowering failed: did not produce a plan.", sbePlan != nullptr);
    uassert(0, "Lowering failed: did not produce any output slots.", !slotMap.empty());

    stage_builder::PlanStageData data{std::make_unique<sbe::RuntimeEnvironment>()};
    data.outputs.set(stage_builder::PlanStageSlots::kResult, slotMap.begin()->second);

    auto planExec =
        uassertStatusOK(plan_executor_factory::make(opCtx,
                                                    nullptr /*cq*/,
                                                    nullptr /*solution*/,
                                                    {std::move(sbePlan), std::move(data)},
                                                    &collection,
                                                    QueryPlannerParams::Options::DEFAULT,
                                                    nss,
                                                    nullptr /*yieldPolicy*/));
    planExec->reattachToOperationContext(opCtx);
    return planExec;
}

std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> createSBEPlanExecutor(
    OperationContext* opCtx,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    const NamespaceString& nss,
    const CollectionPtr& collection,
    const Pipeline& pipeline) {
    const std::string uuidStr = collection->uuid().toString();
    const std::string collNameStr = collection->ns().coll().toString();
    const std::string scanSpecName = collNameStr + "_" + uuidStr;

    PrefixId prefixId;
    const ProjectionName& scanProjName = prefixId.getNextId("scan");
    auto abtTree = translatePipelineToABT(pipeline, scanSpecName, scanProjName, prefixId);

    // TODO: add locks on used indexes?
    auto indexDefs =
        buildIndexSpecsOptimizer(expCtx, opCtx, *collection->getIndexCatalog(), scanProjName);

    const size_t numberOfPartitions = internalQueryDefaultDOP.load();
    // For now handle only local parallelism (no over-the-network exchanges).
    DistributionAndPaths distribution{(numberOfPartitions == 1)
                                          ? DistributionType::Centralized
                                          : DistributionType::UnknownPartitioning};

    auto scanDef = ScanDefinition({{"type", "mongod"},
                                   {"database", collection->ns().db().toString()},
                                   {"uuid", uuidStr},
                                   {ScanNode::kDefaultCollectionNameSpec, collNameStr}},
                                  std::move(indexDefs),
                                  std::move(distribution));

    Metadata metadata({{scanSpecName, std::move(scanDef)}}, numberOfPartitions);

    // TODO: select this from the query.
    constexpr bool useSampling = false;

    if (useSampling) {
        Metadata metadataForSampling = metadata;
        // Do not use indexes for sampling.
        for (auto& [scanDefName, scanDef] : metadataForSampling._scanDefs) {
            scanDef.getIndexDefs().clear();
        }

        // TODO: consider a limited rewrite set.
        OptPhaseManager phaseManagerForSampling(OptPhaseManager::getAllRewritesSet(),
                                                prefixId,
                                                false /*requireRID*/,
                                                std::move(metadataForSampling),
                                                heuristicCE,
                                                DebugInfo::kDefaultForProd);

        const CEType collectionCE = collection->numRecords(opCtx);
        CESamplingTransport samplingCEInstance(opCtx, collectionCE, phaseManagerForSampling);
        auto samplingCEFn = std::bind(&CESamplingTransport::derive,
                                      std::ref(samplingCEInstance),
                                      std::placeholders::_1,
                                      std::placeholders::_2);

        OptPhaseManager phaseManager{OptPhaseManager::getAllRewritesSet(),
                                     prefixId,
                                     false /*requireRID*/,
                                     std::move(metadata),
                                     samplingCEFn,
                                     DebugInfo::kDefaultForProd};
        return optimizeAndCreateExecutor(phaseManager, std::move(abtTree), opCtx, nss, collection);
    }

    // Use heuristics.
    OptPhaseManager phaseManager{OptPhaseManager::getAllRewritesSet(),
                                 prefixId,
                                 std::move(metadata),
                                 DebugInfo::kDefaultForProd};
    return optimizeAndCreateExecutor(phaseManager, std::move(abtTree), opCtx, nss, collection);
}

}  // namespace mongo
