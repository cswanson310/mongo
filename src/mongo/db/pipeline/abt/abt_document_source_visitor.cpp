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

#include "mongo/db/pipeline/abt/abt_document_source_visitor.h"
#include "mongo/db/exec/add_fields_projection_executor.h"
#include "mongo/db/exec/exclusion_projection_executor.h"
#include "mongo/db/exec/inclusion_projection_executor.h"
#include "mongo/db/pipeline/abt/agg_expression_visitor.h"
#include "mongo/db/pipeline/abt/match_expression_visitor.h"
#include "mongo/db/pipeline/abt/utils.h"
#include "mongo/db/pipeline/document_source_bucket_auto.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_change_stream_close_cursor.h"
#include "mongo/db/pipeline/document_source_change_stream_transform.h"
#include "mongo/db/pipeline/document_source_check_invalidate.h"
#include "mongo/db/pipeline/document_source_check_resume_token.h"
#include "mongo/db/pipeline/document_source_coll_stats.h"
#include "mongo/db/pipeline/document_source_current_op.h"
#include "mongo/db/pipeline/document_source_cursor.h"
#include "mongo/db/pipeline/document_source_exchange.h"
#include "mongo/db/pipeline/document_source_facet.h"
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/document_source_geo_near_cursor.h"
#include "mongo/db/pipeline/document_source_graph_lookup.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/db/pipeline/document_source_index_stats.h"
#include "mongo/db/pipeline/document_source_internal_inhibit_optimization.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_internal_split_pipeline.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_list_cached_and_active_users.h"
#include "mongo/db/pipeline/document_source_list_local_sessions.h"
#include "mongo/db/pipeline/document_source_list_sessions.h"
#include "mongo/db/pipeline/document_source_lookup.h"
#include "mongo/db/pipeline/document_source_lookup_change_post_image.h"
#include "mongo/db/pipeline/document_source_lookup_change_pre_image.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/document_source_operation_metrics.h"
#include "mongo/db/pipeline/document_source_out.h"
#include "mongo/db/pipeline/document_source_plan_cache_stats.h"
#include "mongo/db/pipeline/document_source_queue.h"
#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_sample.h"
#include "mongo/db/pipeline/document_source_sample_from_random_cursor.h"
#include "mongo/db/pipeline/document_source_sequential_document_cache.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_skip.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/document_source_tee_consumer.h"
#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "mongo/db/pipeline/visitors/document_source_walker.h"
#include "mongo/db/pipeline/visitors/transformer_interface_walker.h"
#include "mongo/db/query/optimizer/utils.h"
#include "mongo/s/query/document_source_merge_cursors.h"
#include "mongo/s/query/document_source_update_on_add_shard.h"
#include "mongo/util/string_map.h"

namespace mongo::optimizer {

class ABTContext {
public:
    struct NodeWithRootProjection {
        NodeWithRootProjection(ProjectionName rootProjection, ABT node)
            : _rootProjection(std::move(rootProjection)), _node(std::move(node)) {}

        ProjectionName _rootProjection;
        ABT _node;
    };

    ABTContext(PrefixId& prefixId, NodeWithRootProjection node)
        : _node(std::move(node)), _prefixId(prefixId) {
        assertNodeSort(_node._node);
    }

    template <typename T, typename... Args>
    inline auto setNode(ProjectionName rootProjection, Args&&... args) {
        setNode(std::move(rootProjection), std::move(ABT::make<T>(std::forward<Args>(args)...)));
    }

    void setNode(ProjectionName rootProjection, ABT node) {
        assertNodeSort(node);
        _node._node = std::move(node);
        _node._rootProjection = std::move(rootProjection);
    }

    NodeWithRootProjection& getNode() {
        return _node;
    }

    std::string getNextId(const std::string& key) {
        return _prefixId.getNextId(key);
    }

private:
    NodeWithRootProjection _node;

    // We don't own this.
    PrefixId& _prefixId;
};

class ABTTransformerVisitor : public TransformerInterfaceConstVisitor {
    static constexpr const char* rootElement = "$root";

public:
    ABTTransformerVisitor(ABTContext& ctx) : _ctx(ctx) {}

    void visit(const projection_executor::AddFieldsProjectionExecutor* transformer) override {
        visitInclusionNode(transformer->getRoot(), true /*isAddingFields*/);
    }

    void visit(const projection_executor::ExclusionProjectionExecutor* transformer) override {
        visitExclusionNode(*transformer->getRoot());
    }

    void visit(const projection_executor::InclusionProjectionExecutor* transformer) override {
        visitInclusionNode(*transformer->getRoot(), false /*isAddingFields*/);
    }

    void visit(const GroupFromFirstDocumentTransformation* transformer) override {
        // TODO: Is this internal-only?
        unsupportedTransformer(transformer);
    }

    void visit(const ReplaceRootTransformation* transformer) override {
        auto entry = _ctx.getNode();
        const std::string& projName = _ctx.getNextId("newRoot");
        ABT expr = generateAggExpression(
            transformer->getExpression().get(), entry._rootProjection, projName);

        _ctx.setNode<EvaluationNode>(projName, projName, std::move(expr), std::move(entry._node));
    }

    void generateCombinedProjection() const {
        auto it = _fieldMap.find(rootElement);
        if (it == _fieldMap.cend()) {
            return;
        }

        auto entry = _ctx.getNode();
        const std::string& projName = _ctx.getNextId("combinedProjection");
        _ctx.setNode<EvaluationNode>(
            projName,
            projName,
            make<EvalPath>(generatePathForField(it->second), make<Variable>(entry._rootProjection)),
            std::move(entry._node));
    }

private:
    struct FieldMapEntry {
        FieldMapEntry(std::string fieldName) : _fieldName(std::move(fieldName)) {
            uassert(0, "Empty field name", !_fieldName.empty());
        }

        std::string _fieldName;
        bool _hasKeep = false;
        bool _hasLeadingObj = false;
        bool _hasTrailingDefault = false;
        bool _hasDrop = false;
        std::string _constVarName;

        std::set<std::string> _childPaths;
    };

    ABT generatePathForField(const FieldMapEntry& entry) const {
        bool hasLeadingObj = false;
        bool hasTrailingDefault = false;
        std::unordered_set<std::string> keepSet;
        std::unordered_set<std::string> dropSet;
        std::unordered_map<std::string, std::string> varMap;

        for (const std::string& childField : entry._childPaths) {
            const FieldMapEntry& childEntry = _fieldMap.at(childField);
            const std::string& childFieldName = childEntry._fieldName;

            if (childEntry._hasKeep) {
                keepSet.insert(childFieldName);
            }
            if (childEntry._hasDrop) {
                dropSet.insert(childFieldName);
            }
            if (childEntry._hasLeadingObj) {
                hasLeadingObj = true;
            }
            if (childEntry._hasTrailingDefault) {
                hasTrailingDefault = true;
            }
            if (!childEntry._constVarName.empty()) {
                varMap.emplace(childFieldName, childEntry._constVarName);
            }
        }

        ABT result = make<PathIdentity>();
        if (hasLeadingObj) {
            maybeComposePath(result, make<PathObj>());
        }
        if (!keepSet.empty()) {
            maybeComposePath(result, make<PathKeep>(std::move(keepSet)));
        }
        if (!dropSet.empty()) {
            maybeComposePath(result, make<PathDrop>(std::move(dropSet)));
        }

        for (const auto& varMapEntry : varMap) {
            maybeComposePath(
                result,
                make<PathField>(varMapEntry.first,
                                make<PathConstant>(make<Variable>(varMapEntry.second))));
        }

        for (const std::string& childPath : entry._childPaths) {
            const FieldMapEntry& childEntry = _fieldMap.at(childPath);

            ABT childResult = generatePathForField(childEntry);
            if (childResult != make<PathIdentity>()) {
                maybeComposePath(result,
                                 make<PathField>(childEntry._fieldName,
                                                 make<PathTraverse>(std::move(childResult))));
            }
        }

        if (hasTrailingDefault) {
            maybeComposePath(result, make<PathDefault>(Constant::emptyObject()));
        }

        return result;
    }

    using FieldNameIntegrationFn =
        std::function<void(const bool isLastElement, FieldMapEntry& entry)>;

    void integrateFieldPath(const FieldPath& fieldPath, const FieldNameIntegrationFn& fn) {
        std::string path = rootElement;
        auto it = _fieldMap.emplace(path, rootElement);
        const size_t fieldPathLength = fieldPath.getPathLength();

        for (size_t i = 0; i < fieldPathLength; i++) {
            const std::string& fieldName = fieldPath.getFieldName(i).toString();
            path += '.' + fieldName;

            it.first->second._childPaths.insert(path);
            it = _fieldMap.emplace(path, fieldName);
            fn(i == fieldPathLength - 1, it.first->second);
        }
    }

    void unsupportedTransformer(const TransformerInterface* transformer) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Transformer is not supported (code: "
                                << static_cast<int>(transformer->getType()) << ")");
    }

    void processProjectedPaths(const projection_executor::InclusionNode& node) {
        std::set<std::string> preservedPaths;
        node.reportProjectedPaths(&preservedPaths);

        for (const std::string& preservedPathStr : preservedPaths) {
            integrateFieldPath(FieldPath(preservedPathStr),
                               [](const bool isLastElement, FieldMapEntry& entry) {
                                   entry._hasLeadingObj = true;
                                   entry._hasKeep = true;
                               });
        }
    }

    void processComputedPaths(const projection_executor::InclusionNode& node,
                              const std::string& rootProjection,
                              const bool isAddingFields) {
        std::set<std::string> computedPaths;
        StringMap<std::string> renamedPaths;
        node.reportComputedPaths(&computedPaths, &renamedPaths);

        // Handle path renames: essentially single element FieldPath expression.
        for (const auto& renamedPathEntry : renamedPaths) {
            ABT path = translateFieldPath(
                FieldPath(renamedPathEntry.second),
                make<PathIdentity>(),
                [](const std::string& fieldName, const bool isLastElement, ABT input) {
                    return make<PathGet>(fieldName,
                                         isLastElement ? std::move(input)
                                                       : make<PathTraverse>(std::move(input)));
                });

            auto entry = _ctx.getNode();
            const std::string& renamedProjName = _ctx.getNextId("projRenamedPath");
            _ctx.setNode<EvaluationNode>(
                entry._rootProjection,
                renamedProjName,
                make<EvalPath>(std::move(path), make<Variable>(entry._rootProjection)),
                std::move(entry._node));

            integrateFieldPath(FieldPath(renamedPathEntry.first),
                               [&renamedProjName, &isAddingFields](const bool isLastElement,
                                                                   FieldMapEntry& entry) {
                                   if (!isAddingFields) {
                                       entry._hasKeep = true;
                                   }
                                   if (isLastElement) {
                                       entry._constVarName = renamedProjName;
                                       entry._hasTrailingDefault = true;
                                   }
                               });
        }

        // Handle general expression projection.
        for (const std::string& computedPathStr : computedPaths) {
            const FieldPath computedPath(computedPathStr);

            auto entry = _ctx.getNode();
            const std::string& getProjName = _ctx.getNextId("projGetPath");
            ABT getExpr = generateAggExpression(
                node.getExpressionForPath(computedPath).get(), rootProjection, getProjName);

            _ctx.setNode<EvaluationNode>(std::move(entry._rootProjection),
                                         getProjName,
                                         std::move(getExpr),
                                         std::move(entry._node));

            integrateFieldPath(
                computedPath,
                [&getProjName, &isAddingFields](const bool isLastElement, FieldMapEntry& entry) {
                    if (!isAddingFields) {
                        entry._hasKeep = true;
                    }
                    if (isLastElement) {
                        entry._constVarName = getProjName;
                        entry._hasTrailingDefault = true;
                    }
                });
        }
    }

    void visitInclusionNode(const projection_executor::InclusionNode& node,
                            const bool isAddingFields) {
        auto entry = _ctx.getNode();
        const std::string rootProjection = entry._rootProjection;

        processProjectedPaths(node);
        processComputedPaths(node, rootProjection, isAddingFields);
    }

    void visitExclusionNode(const projection_executor::ExclusionNode& node) {
        std::set<std::string> preservedPaths;
        node.reportProjectedPaths(&preservedPaths);

        for (const std::string& preservedPathStr : preservedPaths) {
            integrateFieldPath(FieldPath(preservedPathStr),
                               [](const bool isLastElement, FieldMapEntry& entry) {
                                   if (isLastElement) {
                                       entry._hasDrop = true;
                                   }
                               });
        }
    }

    ABTContext& _ctx;

    std::unordered_map<std::string, FieldMapEntry> _fieldMap;
};

class ABTTranslateDocumentSourceVisitor : public DocumentSourceConstVisitor {
public:
    ABTTranslateDocumentSourceVisitor(ABTContext& ctx) : _ctx(ctx) {}

    void visit(const DocumentSourceBucketAuto* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCloseCursor* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceChangeStreamTransform* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCheckInvalidate* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCheckResumability* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceEnsureResumeTokenPresent* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCollStats* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCurrentOp* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceCursor* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceExchange* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceFacet* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceGeoNear* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceGeoNearCursor* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceGraphLookUp* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceGroup* source) override {
        const StringMap<boost::intrusive_ptr<Expression>>& idFields = source->getIdFields();
        if (idFields.size() != 1) {
            // Unsupported for other cases.
            unsupportedStage(source);
            return;
        }
        const std::vector<AccumulationStatement>& accumulatedFields =
            source->getAccumulatedFields();

        auto entry = _ctx.getNode();
        const auto& idField = idFields.cbegin();
        const std::string& groupByField = idField->first;

        const std::string& groupByProjName = _ctx.getNextId("groupByProj");
        ABT groupByExpr =
            generateAggExpression(idField->second.get(), entry._rootProjection, groupByProjName);

        _ctx.setNode<EvaluationNode>(
            entry._rootProjection, groupByProjName, std::move(groupByExpr), std::move(entry._node));
        entry = _ctx.getNode();

        ProjectionNameVector aggregationProjectionFieldNames;
        ProjectionNameVector aggregationProjectionNames;
        ABTVector aggregationProjections;
        for (const AccumulationStatement& stmt : accumulatedFields) {
            const std::string& fieldName = stmt.fieldName;
            aggregationProjectionFieldNames.push_back(fieldName);

            const std::string aggProjName = _ctx.getNextId(fieldName + "_agg");
            aggregationProjectionNames.push_back(aggProjName);

            ABT aggInput =
                generateAggExpression(stmt.expr.argument.get(), entry._rootProjection, aggProjName);
            aggregationProjections.emplace_back(make<FunctionCall>(
                stmt.makeAccumulator()->getOpName(), ABTVector{std::move(aggInput)}));
        }

        _ctx.setNode<GroupByNode>(groupByProjName,
                                  ProjectionNameVector{groupByProjName},
                                  aggregationProjectionNames,
                                  aggregationProjections,
                                  std::move(entry._node));

        ABT integrationPath =
            make<PathField>(std::move(groupByField),
                            make<PathConstant>(make<Variable>(std::move(groupByProjName))));
        for (size_t i = 0; i < aggregationProjectionFieldNames.size(); i++) {
            integrationPath = make<PathComposeM>(
                make<PathField>(
                    aggregationProjectionFieldNames.at(i),
                    make<PathConstant>(make<Variable>(aggregationProjectionNames.at(i)))),
                std::move(integrationPath));
        }

        entry = _ctx.getNode();
        const std::string& mergeProject = _ctx.getNextId("agg_project");
        _ctx.setNode<EvaluationNode>(
            mergeProject,
            mergeProject,
            make<EvalPath>(std::move(integrationPath), Constant::emptyObject()),
            std::move(entry._node));
    }

    void visit(const DocumentSourceIndexStats* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceInternalInhibitOptimization* source) override {
        // Can be ignored.
    }

    void visit(const DocumentSourceInternalShardFilter* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceInternalSplitPipeline* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceLimit* source) override {
        pushLimitSkip(source->getLimit(), 0);
    }

    void visit(const DocumentSourceListCachedAndActiveUsers* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceListLocalSessions* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceListSessions* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceLookUp* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceLookupChangePostImage* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceLookupChangePreImage* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceMatch* source) override {
        auto entry = _ctx.getNode();
        ABT matchExpr = generateMatchExpression(source->getMatchExpression(),
                                                true /*allowAggExpressions*/,
                                                entry._rootProjection,
                                                _ctx.getNextId("matchExpression"));

        _ctx.setNode<FilterNode>(
            entry._rootProjection,
            make<EvalFilter>(std::move(matchExpr), make<Variable>(entry._rootProjection)),
            std::move(entry._node));
    }

    void visit(const DocumentSourceMerge* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceMergeCursors* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceOperationMetrics* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceOplogMatch* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceOut* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourcePlanCacheStats* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceQueue* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceRedact* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceSample* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceSampleFromRandomCursor* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceSequentialDocumentCache* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceSingleDocumentTransformation* source) override {
        ABTTransformerVisitor visitor(_ctx);
        TransformerInterfaceWalker walker(&visitor);
        walker.walk(&source->getTransformer());
        visitor.generateCombinedProjection();
    }

    void visit(const DocumentSourceSkip* source) override {
        pushLimitSkip(-1, source->getSkip());
    }

    void visit(const DocumentSourceSort* source) override {
        if (source->getLimit().has_value()) {
            // Already has sort merged into it.
            pushLimitSkip(source->getLimit().value(), 0);
        }

        ProjectionCollationSpec collationSpec;
        const SortPattern& pattern = source->getSortKeyPattern();
        for (size_t i = 0; i < pattern.size(); i++) {
            const SortPattern::SortPatternPart& part = pattern[i];
            if (!part.fieldPath.has_value()) {
                // TODO: consider metadata expression.
                continue;
            }

            const std::string& sortProjName = _ctx.getNextId("sort");
            collationSpec.emplace_back(
                sortProjName, part.isAscending ? CollationOp::Ascending : CollationOp::Descending);

            const FieldPath& fieldPath = part.fieldPath.value();
            ABT sortPath = make<PathIdentity>();
            for (size_t j = 0; j < fieldPath.getPathLength(); j++) {
                sortPath = make<PathGet>(fieldPath.getFieldName(j).toString(), std::move(sortPath));
            }

            auto entry = _ctx.getNode();
            _ctx.setNode<EvaluationNode>(
                entry._rootProjection,
                sortProjName,
                make<EvalPath>(std::move(sortPath), make<Variable>(entry._rootProjection)),
                std::move(entry._node));
        }

        if (!collationSpec.empty()) {
            auto entry = _ctx.getNode();
            _ctx.setNode<CollationNode>(std::move(entry._rootProjection),
                                        properties::CollationRequirement(std::move(collationSpec)),
                                        std::move(entry._node));
        }
    }

    void visit(const DocumentSourceTeeConsumer* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceUnionWith* source) override {
        unsupportedStage(source);
    }

    void visit(const DocumentSourceUnwind* source) override {
        const FieldPath& unwindFieldPath = source->getUnwindPath();
        const bool preserveNullAndEmpty = source->preserveNullAndEmptyArrays();

        const std::string pidProjName = _ctx.getNextId("unwoundPid");
        const std::string unwoundProjName = _ctx.getNextId("unwoundProj");

        const auto generatePidGteZeroTest = [&pidProjName](ABT thenCond, ABT elseCond) {
            return make<If>(
                make<BinaryOp>(Operations::Gte, make<Variable>(pidProjName), Constant::int64(0)),
                std::move(thenCond),
                std::move(elseCond));
        };

        ABT embedPath = make<Variable>(unwoundProjName);
        if (preserveNullAndEmpty) {
            const std::string unwindLambdaVarName = _ctx.getNextId("unwoundLambdaVarName");
            embedPath = make<PathLambda>(make<LambdaAbstraction>(
                unwindLambdaVarName,
                generatePidGteZeroTest(std::move(embedPath), make<Variable>(unwindLambdaVarName))));
        } else {
            embedPath = make<PathConstant>(std::move(embedPath));
        }
        embedPath = translateFieldPath(
            unwindFieldPath,
            std::move(embedPath),
            [](const std::string& fieldName, const bool isLastElement, ABT input) {
                return make<PathField>(fieldName,
                                       isLastElement ? std::move(input)
                                                     : make<PathTraverse>(std::move(input)));
            });

        ABT unwoundPath = translateFieldPath(
            unwindFieldPath,
            make<PathIdentity>(),
            [](const std::string& fieldName, const bool isLastElement, ABT input) {
                return make<PathGet>(fieldName, std::move(input));
            });

        auto entry = _ctx.getNode();
        _ctx.setNode<EvaluationNode>(
            entry._rootProjection,
            unwoundProjName,
            make<EvalPath>(std::move(unwoundPath), make<Variable>(entry._rootProjection)),
            std::move(entry._node));

        entry = _ctx.getNode();
        _ctx.setNode<UnwindNode>(std::move(entry._rootProjection),
                                 unwoundProjName,
                                 pidProjName,
                                 preserveNullAndEmpty,
                                 std::move(entry._node));

        entry = _ctx.getNode();
        const std::string embedProjName = _ctx.getNextId("embedProj");
        _ctx.setNode<EvaluationNode>(
            embedProjName,
            embedProjName,
            make<EvalPath>(std::move(embedPath), make<Variable>(entry._rootProjection)),
            std::move(entry._node));

        if (source->indexPath().has_value()) {
            const FieldPath indexFieldPath = source->indexPath().get();
            if (indexFieldPath.getPathLength() > 0) {
                ABT indexPath = translateFieldPath(
                    indexFieldPath,
                    make<PathConstant>(
                        generatePidGteZeroTest(make<Variable>(pidProjName), Constant::null())),
                    [](const std::string& fieldName, const bool isLastElement, ABT input) {
                        return make<PathField>(fieldName, std::move(input));
                    });

                entry = _ctx.getNode();
                const std::string embedPidProjName = _ctx.getNextId("embedPidProj");
                _ctx.setNode<EvaluationNode>(
                    embedPidProjName,
                    embedPidProjName,
                    make<EvalPath>(std::move(indexPath), make<Variable>(entry._rootProjection)),
                    std::move(entry._node));
            }
        }
    }

    void visit(const DocumentSourceUpdateOnAddShard* source) override {
        unsupportedStage(source);
    }

private:
    void unsupportedStage(const DocumentSource* source) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Stage is not supported: " << source->getSourceName());
    }

    void pushLimitSkip(const properties::LimitSkipRequirement::IntType limit,
                       const properties::LimitSkipRequirement::IntType skip) {
        auto entry = _ctx.getNode();
        _ctx.setNode<LimitSkipNode>(std::move(entry._rootProjection),
                                    properties::LimitSkipRequirement(limit, skip),
                                    std::move(entry._node));
    }

    ABTContext& _ctx;
};

ABT translatePipelineToABT(const Pipeline& pipeline,
                           std::string scanDefName,
                           ProjectionName scanProjName,
                           PrefixId& prefixId) {
    ABTContext ctx(prefixId,
                   ABTContext::NodeWithRootProjection{
                       scanProjName, make<ScanNode>(scanProjName, std::move(scanDefName))});
    ABTTranslateDocumentSourceVisitor visitor(ctx);

    DocumentSourceWalker walker(nullptr /*preVisitor*/, &visitor);
    walker.walk(pipeline);

    auto entry = ctx.getNode();
    return make<RootNode>(
        properties::ProjectionRequirement{ProjectionNameVector{std::move(entry._rootProjection)}},
        std::move(entry._node));
}

}  // namespace mongo::optimizer
