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
#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_union_with.h"

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
    : DocumentSource(kStageName, expCtx),
      _unionNss(std::move(unionNss)),
      _rawPipeline(std::move(pipeline)) {

    // Prepend any stages from a view definition.
    const auto& resolvedNamespace = expCtx->getResolvedNamespace(_unionNss);
    _resolvedNss = resolvedNamespace.ns;

    // Copy the ExpressionContext of the base aggregation, using the inner namespace instead.
    _unionExpCtx = expCtx->copyWith(_resolvedNss);

    _rawPipeline.insert(
        _rawPipeline.begin(), resolvedNamespace.pipeline.begin(), resolvedNamespace.pipeline.end());
    // TODO SERVER-XXXX: This can't happen here in a sharded cluster, since it attaches a
    // non-serializable $cursor stage.
    _pipeline = pExpCtx->mongoProcessInterface->makePipeline(_rawPipeline, _unionExpCtx);
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
    if (!_sourceExhausted) {
        auto nextInput = pSource->getNext();
        if (!nextInput.isEOF()) {
            return nextInput;
        }
        _sourceExhausted = true;
        // Drop down to iterate the sub-pipe
    }

    if (_pipeline) {
        auto res = _pipeline->getNext();
        if (res)
            return std::move(*res);
    }

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

void DocumentSourceUnionWith::serializeToArray(
    std::vector<Value>& array, boost::optional<ExplainOptions::Verbosity> explain) const {

    Document doc = DOC("coll" << _unionNss.coll());
    array.push_back(Value(doc));
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
