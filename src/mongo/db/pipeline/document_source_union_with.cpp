/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/util/log.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE(unionWith,
                         DocumentSourceUnionWith::LiteParsed::parse,
                         DocumentSourceUnionWith::createFromBson);

std::unique_ptr<DocumentSourceUnionWith::LiteParsed> DocumentSourceUnionWith::LiteParsed::parse(
    const AggregationRequest& request, const BSONElement& spec) {
    uassert(ErrorCodes::FailedToParse,
            str::stream()
                << "the $unionWith stage specification must be an object or string, but found "
                << typeName(spec.type()),
            spec.type() == BSONType::Object || spec.type() == BSONType::String);

    NamespaceString unionNss;
    stdx::unordered_set<NamespaceString> foreignNssSet;
    boost::optional<LiteParsedPipeline> liteParsedPipeline;
    if (spec.type() == BSONType::String) {
        unionNss = NamespaceString(request.getNamespaceString().db(), spec.valueStringData());
    } else {
        unionNss =
            NamespaceString(request.getNamespaceString().db(), spec["coll"].valueStringData());

        // Recursively lite parse the nested pipeline, if one exists.
        if (auto pipelineElem = spec["pipeline"]) {
            auto pipeline =
                uassertStatusOK(AggregationRequest::parsePipelineFromBSON(pipelineElem));
            AggregationRequest foreignAggReq(unionNss, std::move(pipeline));
            liteParsedPipeline = LiteParsedPipeline(foreignAggReq);

            auto pipelineInvolvedNamespaces = liteParsedPipeline->getInvolvedNamespaces();
            foreignNssSet.insert(pipelineInvolvedNamespaces.begin(),
                                 pipelineInvolvedNamespaces.end());
        }
    }

    foreignNssSet.insert(unionNss);

    return std::make_unique<DocumentSourceUnionWith::LiteParsed>(
        std::move(unionNss), std::move(foreignNssSet), std::move(liteParsedPipeline));
}

DocumentSourceUnionWith::DocumentSourceUnionWith(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    NamespaceString unionNss,
    std::vector<BSONObj> pipeline)
    : DocumentSource(kStageName, expCtx), _rawPipeline(std::move(pipeline)) {

    auto resolvedNs = expCtx->getResolvedNamespace(unionNss);
    addViewDefinition(std::move(resolvedNs.ns), std::move(resolvedNs.pipeline));
}

void DocumentSourceUnionWith::addViewDefinition(NamespaceString nss,
                                                std::vector<BSONObj> viewPipeline) {

    _unionNss = std::move(nss);
    // Copy the ExpressionContext of the base aggregation, using the inner namespace instead.
    _unionExpCtx = pExpCtx->copyWith(_unionNss);
    _rawPipeline.insert(_rawPipeline.begin(), viewPipeline.begin(), viewPipeline.end());
    MongoProcessInterface::MakePipelineOptions opts;
    opts.attachCursorSource = false;
    _pipeline = pExpCtx->mongoProcessInterface->makePipeline(_rawPipeline, _unionExpCtx, opts);
}

boost::intrusive_ptr<DocumentSource> DocumentSourceUnionWith::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream()
                << "the $unionWith stage specification must be an object or string, but found "
                << typeName(elem.type()),
            elem.type() == BSONType::Object || elem.type() == BSONType::String);

    NamespaceString unionNss;
    std::vector<BSONObj> pipeline;
    if (elem.type() == BSONType::String) {
        unionNss = NamespaceString(expCtx->ns.db().toString(), elem.valueStringData());
    } else {
        for (auto&& arg : elem.embeddedObject()) {
            auto fieldName = arg.fieldNameStringData();
            if (fieldName == "coll")
                unionNss = NamespaceString(expCtx->ns.db().toString(), arg.valueStringData());
            else if (fieldName == "pipeline") {
                pipeline = uassertStatusOK(AggregationRequest::parsePipelineFromBSON(arg));
            } else
                uasserted(ErrorCodes::FailedToParse,
                          str::stream()
                              << "Unknown argument given to $unionWith stage: " << fieldName);
        }
    }
    return make_intrusive<DocumentSourceUnionWith>(
        expCtx, std::move(unionNss), std::move(pipeline));
}

DocumentSource::GetNextResult DocumentSourceUnionWith::doGetNext() {
    if (_executionState == ExecutionProgress::kIteratingSource) {
        auto nextInput = pSource->getNext();
        if (!nextInput.isEOF()) {
            return nextInput;
        }
        _executionState = ExecutionProgress::kStartingSubPipeline;
        // Drop down to iterate the sub-pipe
    }

    // This has to happen here and not in create because if the document source is created on
    // mongos this would add a non-serializable cursor stage. Here it will only happen on mongod.
    if (_executionState == ExecutionProgress::kStartingSubPipeline) {
        LOG(5) << "$unionWith attaching cursor to pipeline " << Value(_pipeline->serialize());
        try {
            _pipeline = pExpCtx->mongoProcessInterface->attachCursorSourceToPipeline(
                _unionExpCtx, _pipeline.release());
            _executionState = ExecutionProgress::kIteratingSubPipeline;
        } catch (const ExceptionFor<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod>& e) {

            LOG(5) << "$unionWith found view definition. ns: " << e->getNamespace()
                   << ", pipeline: " << Value(e->getPipeline());
            addViewDefinition(e->getNamespace(), e->getPipeline());
            return doGetNext();
        }
    }
    invariant(_pipeline);
    auto res = _pipeline->getNext();
    if (res)
        return std::move(*res);

    _executionState = ExecutionProgress::kFinished;
    return GetNextResult::makeEOF();
}

DocumentSource::GetModPathsReturn DocumentSourceUnionWith::getModifiedPaths() const {
    return {GetModPathsReturn::Type::kAllPaths, {}, {}};
}

void DocumentSourceUnionWith::doDispose() {
    if (_pipeline) {
        _pipeline->dispose(pExpCtx->opCtx);
        _pipeline.reset();
    }
}

Value DocumentSourceUnionWith::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    auto subPipeline = explain ? _pipeline->writeExplainOps(*explain) : _pipeline->serialize();
    return Value{Document{
        {kStageName, Document{{"coll"_sd, _unionNss.coll()}, {"pipeline"_sd, subPipeline}}}}};
}

DepsTracker::State DocumentSourceUnionWith::getDependencies(DepsTracker* deps) const {
    return DepsTracker::State::SEE_NEXT;
}

void DocumentSourceUnionWith::detachFromOperationContext() {
    if (_pipeline) {
        // We have a pipeline we're going to be executing across multiple calls to getNext(), so we
        // use Pipeline::detachFromOperationContext() to take care of updating
        // '_unionExpCtx->opCtx'.
        _pipeline->detachFromOperationContext();
    } else if (_unionExpCtx) {
        _unionExpCtx->opCtx = nullptr;
    }
}

void DocumentSourceUnionWith::reattachToOperationContext(OperationContext* opCtx) {
    if (_pipeline) {
        // We have a pipeline we're going to be executing across multiple calls to getNext(), so we
        // use Pipeline::reattachToOperationContext() to take care of updating
        // '_unionExpCtx->opCtx'.
        _pipeline->reattachToOperationContext(opCtx);
        invariant(_unionExpCtx->opCtx == opCtx);
    } else if (_unionExpCtx) {
        _unionExpCtx->opCtx = opCtx;
    }
}

void DocumentSourceUnionWith::addInvolvedCollections(
    stdx::unordered_set<NamespaceString>* collectionNames) const {
    collectionNames->insert(_unionNss);
    for (auto&& stage : _pipeline->getSources()) {
        stage->addInvolvedCollections(collectionNames);
    }
}

}  // namespace mongo
