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

#include "mongo/db/exec/sbe/abt/sbe_abt_test_util.h"
#include "mongo/db/pipeline/abt/abt_document_source_visitor.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/unittest/temp_dir.h"

namespace mongo::optimizer {

static std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> parsePipelineInternal(
    NamespaceString nss, const std::string& inputPipeline, OperationContextNoop& opCtx) {
    const BSONObj inputBson = fromjson("{pipeline: " + inputPipeline + "}");

    std::vector<BSONObj> rawPipeline;
    for (auto&& stageElem : inputBson["pipeline"].Array()) {
        ASSERT_EQUALS(stageElem.type(), BSONType::Object);
        rawPipeline.push_back(stageElem.embeddedObject());
    }

    AggregateCommandRequest request(std::move(nss), rawPipeline);
    boost::intrusive_ptr<ExpressionContextForTest> ctx(
        new ExpressionContextForTest(&opCtx, request));

    unittest::TempDir tempDir("ABTPipelineTest");
    ctx->tempDir = tempDir.path();

    return Pipeline::parse(request.getPipeline(), ctx);
}

std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> parsePipeline(
    const std::string& pipelineStr, NamespaceString nss) {
    OperationContextNoop opCtx;
    return parsePipelineInternal(std::move(nss), pipelineStr, opCtx);
}

ABT translatePipeline(const std::string& pipelineStr,
                      std::string scanDefName,
                      NamespaceString nss,
                      PrefixId prefixId) {
    auto pipeline = parsePipeline(pipelineStr, std::move(nss));
    return translatePipelineToABT(
        *pipeline.get(), std::move(scanDefName), prefixId.getNextId("scan"), prefixId);
}

}  // namespace mongo::optimizer
