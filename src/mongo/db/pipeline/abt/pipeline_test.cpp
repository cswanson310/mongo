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

#include <boost/intrusive_ptr.hpp>

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/pipeline/abt/abt_document_source_visitor.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using namespace optimizer;

std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> parsePipeline(
    const NamespaceString& nss, const std::string& inputPipeline, OperationContextNoop& opCtx) {
    const BSONObj inputBson = fromjson("{pipeline: " + inputPipeline + "}");

    std::vector<BSONObj> rawPipeline;
    for (auto&& stageElem : inputBson["pipeline"].Array()) {
        ASSERT_EQUALS(stageElem.type(), BSONType::Object);
        rawPipeline.push_back(stageElem.embeddedObject());
    }

    AggregateCommand request(nss, rawPipeline);
    boost::intrusive_ptr<ExpressionContextForTest> ctx(
        new ExpressionContextForTest(&opCtx, request));

    unittest::TempDir tempDir("ABTPipelineTest");
    ctx->tempDir = tempDir.path();

    return Pipeline::parse(request.getPipeline(), ctx);
}

ABT translatePipeline(const std::string& pipelineStr, std::string scanDefName, PrefixId& prefixId) {
    OperationContextNoop opCtx;
    auto pipeline = parsePipeline(NamespaceString("a.collection"), pipelineStr, opCtx);
    return translatePipelineToABT(
        *pipeline.get(), std::move(scanDefName), prefixId.getNextId("scan"), prefixId);
}

ABT translatePipeline(const std::string& pipelineStr, std::string scanDefName = "collection") {
    PrefixId prefixId;
    return translatePipeline(pipelineStr, std::move(scanDefName), prefixId);
}

TEST(ABTTranslate, SortLimitSkip) {
    ABT translated = translatePipeline(
        "[{$limit: 5}, "
        "{$skip: 3}, "
        "{$sort: {a: 1, b: -1}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       sort_0: Ascending\n"
        "|   |       sort_1: Descending\n"
        "|   RefBlock: \n"
        "|       Variable [sort_0]\n"
        "|       Variable [sort_1]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [sort_1]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [sort_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "LimitSkipNode []\n"
        "|   limitSkip: []\n"
        "|       limit: (none)\n"
        "|       skip: 3\n"
        "LimitSkipNode []\n"
        "|   limitSkip: []\n"
        "|       limit: 5\n"
        "|       skip: 0\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectRetain) {
    ABT translated = translatePipeline("[{$project: {a: 1, b: 1}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathObj [] \n"
        "|           |   PathKeep [_id, a, b]\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, AddFields) {
    // Since '$z' is a single element, it will be considered a renamed path.
    ABT translated = translatePipeline("[{$addFields: {a: '$z'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathField [a]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projRenamedPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projRenamedPath_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [z]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectRenames) {
    // Since '$c' is a single element, it will be considered a renamed path.
    ABT translated = translatePipeline("[{$project: {'a.b': '$c'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathObj [] \n"
        "|           |   |   PathKeep [_id, a]\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathKeep [b]\n"
        "|           |   |   PathField [b]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projRenamedPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projRenamedPath_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [c]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectPaths) {
    ABT translated = translatePipeline("[{$project: {'a.b.c': '$x.y.z'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathObj [] \n"
        "|           |   |   PathKeep [_id, a]\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathKeep [b]\n"
        "|           |   PathField [b]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathKeep [c]\n"
        "|           |   |   PathField [c]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projGetPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [x]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathGet [y]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathGet [z]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectPaths1) {
    ABT translated = translatePipeline("[{$project: {'a.b':1, 'a.c':1, 'b':1}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathObj [] \n"
        "|           |   |   PathKeep [_id, a, b]\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathObj [] \n"
        "|           |   PathKeep [b, c]\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectInclusion) {
    ABT translated = translatePipeline("[{$project: {a: {$add: ['$c.d', 2]}, b: 1}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathObj [] \n"
        "|           |   |   |   PathKeep [_id, a, b]\n"
        "|           |   |   PathField [a]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projGetPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           BinaryOp [Add]\n"
        "|           |   Const [2]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [c]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathGet [d]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectExclusion) {
    ABT translated = translatePipeline("[{$project: {a: 0, b: 0}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathDrop [a, b]\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectReplaceRoot) {
    ABT translated = translatePipeline("[{$replaceRoot: {newRoot: '$a'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       newRoot_0\n"
        "|   RefBlock: \n"
        "|       Variable [newRoot_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [newRoot_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, MatchBasic) {
    ABT translated = translatePipeline("[{$match: {a: 1, b: 2}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathComposeM []\n"
        "|   |   |   PathGet [a]\n"
        "|   |   |   PathTraverse []\n"
        "|   |   |   PathCompare [Eq] \n"
        "|   |   |   Const [1]\n"
        "|   |   PathGet [b]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [2]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, MatchPath) {
    ABT translated = translatePipeline("[{$match: {$expr: {$eq: ['$a.b', 1]}}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathConstant []\n"
        "|   |   Let [matchExpression_0_coerceToBool_0]\n"
        "|   |   |   BinaryOp [Eq]\n"
        "|   |   |   |   EvalPath []\n"
        "|   |   |   |   |   PathGet [a]\n"
        "|   |   |   |   |   PathTraverse []\n"
        "|   |   |   |   |   PathGet [b]\n"
        "|   |   |   |   |   PathIdentity []\n"
        "|   |   |   |   Variable [scan_0]\n"
        "|   |   |   Const [1]\n"
        "|   |   BinaryOp [And]\n"
        "|   |   |   BinaryOp [Neq]\n"
        "|   |   |   |   BinaryOp [Cmp3w]\n"
        "|   |   |   |   |   Variable [matchExpression_0_coerceToBool_0]\n"
        "|   |   |   |   Const [0]\n"
        "|   |   |   Const [0]\n"
        "|   |   BinaryOp [And]\n"
        "|   |   |   BinaryOp [Neq]\n"
        "|   |   |   |   BinaryOp [Cmp3w]\n"
        "|   |   |   |   |   Variable [matchExpression_0_coerceToBool_0]\n"
        "|   |   |   |   Const [false]\n"
        "|   |   |   Const [0]\n"
        "|   |   BinaryOp [And]\n"
        "|   |   |   UnaryOp [Not]\n"
        "|   |   |   FunctionCall [isNull]\n"
        "|   |   |   BinaryOp [Eq]\n"
        "|   |   |   |   EvalPath []\n"
        "|   |   |   |   |   PathGet [a]\n"
        "|   |   |   |   |   PathTraverse []\n"
        "|   |   |   |   |   PathGet [b]\n"
        "|   |   |   |   |   PathIdentity []\n"
        "|   |   |   |   Variable [scan_0]\n"
        "|   |   |   Const [1]\n"
        "|   |   FunctionCall [exists]\n"
        "|   |   BinaryOp [Eq]\n"
        "|   |   |   EvalPath []\n"
        "|   |   |   |   PathGet [a]\n"
        "|   |   |   |   PathTraverse []\n"
        "|   |   |   |   PathGet [b]\n"
        "|   |   |   |   PathIdentity []\n"
        "|   |   |   Variable [scan_0]\n"
        "|   |   Const [1]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ElemMatchPath) {
    ABT translated = translatePipeline(
        "[{$project: {a: {$literal: [1, 2, 3, 4]}}}, {$match: {a: {$elemMatch: {$gte: 2, $lte: "
        "3}}}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathComposeM []\n"
        "|   |   |   PathCompare [Lte] \n"
        "|   |   |   Const [3]\n"
        "|   |   PathCompare [Gte] \n"
        "|   |   Const [2]\n"
        "|   Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathObj [] \n"
        "|           |   |   |   PathKeep [_id, a]\n"
        "|           |   |   PathField [a]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projGetPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           Const [[1, 2, 3, 4]]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, MatchProject) {
    ABT translated = translatePipeline(
        "[{$project: {s: {$add: ['$a', '$b']}, c: 1}}, "
        "{$match: {$or: [{c: 2}, {s: {$gte: 10}}]}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathComposeA []\n"
        "|   |   |   PathGet [c]\n"
        "|   |   |   PathTraverse []\n"
        "|   |   |   PathCompare [Eq] \n"
        "|   |   |   Const [2]\n"
        "|   |   PathGet [s]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Gte] \n"
        "|   |   Const [10]\n"
        "|   Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathObj [] \n"
        "|           |   |   |   PathKeep [_id, c, s]\n"
        "|           |   |   PathField [s]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projGetPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           BinaryOp [Add]\n"
        "|           |   EvalPath []\n"
        "|           |   |   PathGet [b]\n"
        "|           |   |   PathIdentity []\n"
        "|           |   Variable [scan_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ProjectComplex) {
    ABT translated = translatePipeline("[{$project: {'a1.b.c':1, 'a.b.c.d.e':'str'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathObj [] \n"
        "|           |   |   |   PathKeep [_id, a, a1]\n"
        "|           |   |   PathField [a]\n"
        "|           |   |   PathTraverse []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathKeep [b]\n"
        "|           |   |   PathField [b]\n"
        "|           |   |   PathTraverse []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathKeep [c]\n"
        "|           |   |   PathField [c]\n"
        "|           |   |   PathTraverse []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathKeep [d]\n"
        "|           |   |   PathField [d]\n"
        "|           |   |   PathTraverse []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathKeep [e]\n"
        "|           |   |   |   PathField [e]\n"
        "|           |   |   |   PathConstant []\n"
        "|           |   |   |   Variable [projGetPath_0]\n"
        "|           |   |   PathDefault [] \n"
        "|           |   |   Const [{}]\n"
        "|           |   PathField [a1]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathObj [] \n"
        "|           |   |   PathKeep [b]\n"
        "|           |   PathField [b]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathObj [] \n"
        "|           |   PathKeep [c]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           Const [\"str\"]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, ExprFilter) {
    ABT translated = translatePipeline(
        "[{$project: {a: {$filter: {input: [1, 2, 'str', {a: 2.0, b:'s'}, 3, 4], as: 'num', cond: "
        "{$and: [{$gte: ['$$num', 2]}, {$lte: ['$$num', 3]}]}}}}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       combinedProjection_0\n"
        "|   RefBlock: \n"
        "|       Variable [combinedProjection_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [combinedProjection_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathComposeM []\n"
        "|           |   |   |   PathComposeM []\n"
        "|           |   |   |   |   PathObj [] \n"
        "|           |   |   |   PathKeep [_id, a]\n"
        "|           |   |   PathField [a]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [projGetPath_0]\n"
        "|           |   PathDefault [] \n"
        "|           |   Const [{}]\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [projGetPath_0]\n"
        "|           EvalPath []\n"
        "|           |   PathTraverse []\n"
        "|           |   PathLambda []\n"
        "|           |   LambdaAbstraction [projGetPath_0_var_1]\n"
        "|           |   If []\n"
        "|           |   |   |   Let [projGetPath_0_coerceToBool_2]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   Let [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   Let [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [2]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [2]\n"
        "|           |   |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   Const [2]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_2]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   Const [0]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_2]\n"
        "|           |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   Const [0]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   Let [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   Let [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [2]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [2]\n"
        "|           |   |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   Const [2]\n"
        "|           |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   Let [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_0]\n"
        "|           |   |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   |   Const [3]\n"
        "|           |   |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   |   BinaryOp [Lte]\n"
        "|           |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   Const [3]\n"
        "|           |   |   |   Let [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   Const [2]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   Const [0]\n"
        "|           |   |   |   |   Const [0]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   BinaryOp [Neq]\n"
        "|           |   |   |   |   |   BinaryOp [Cmp3w]\n"
        "|           |   |   |   |   |   |   Variable [projGetPath_0_coerceToBool_1]\n"
        "|           |   |   |   |   |   Const [false]\n"
        "|           |   |   |   |   Const [0]\n"
        "|           |   |   |   BinaryOp [And]\n"
        "|           |   |   |   |   UnaryOp [Not]\n"
        "|           |   |   |   |   FunctionCall [isNull]\n"
        "|           |   |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   |   Const [2]\n"
        "|           |   |   |   FunctionCall [exists]\n"
        "|           |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   |   |   Const [2]\n"
        "|           |   |   Variable [projGetPath_0_var_1]\n"
        "|           |   Const [Nothing]\n"
        "|           Const [[1, 2, \"str\", {\"a\" : 2, \"b\" : \"s\"}, 3, 4]]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, GroupBasic) {
    ABT translated =
        translatePipeline("[{$group: {_id: '$a.b', s: {$sum: {$multiply: ['$b', '$c']}}}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       agg_project_0\n"
        "|   RefBlock: \n"
        "|       Variable [agg_project_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [agg_project_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathField [s]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [s_agg_0]\n"
        "|           |   PathField [_id]\n"
        "|           |   PathConstant []\n"
        "|           |   Variable [groupByProj_0]\n"
        "|           Const [{}]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [groupByProj_0]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [s_agg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           BinaryOp [Mult]\n"
        "|           |   EvalPath []\n"
        "|           |   |   PathGet [c]\n"
        "|           |   |   PathIdentity []\n"
        "|           |   Variable [scan_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [groupByProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, GroupLocalGlobal) {
    ABT translated = translatePipeline("[{$group: {_id: '$a', c: {$sum: '$b'}}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       agg_project_0\n"
        "|   RefBlock: \n"
        "|       Variable [agg_project_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [agg_project_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathField [c]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [c_agg_0]\n"
        "|           |   PathField [_id]\n"
        "|           |   PathConstant []\n"
        "|           |   Variable [groupByProj_0]\n"
        "|           Const [{}]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [groupByProj_0]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [c_agg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [groupByProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));

    PrefixId prefixId;
    std::string scanDefName = "collection";
    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{scanDefName, ScanDefinition{{}, {}, {DistributionType::UnknownPartitioning}}}},
         5 /*numberOfPartitions*/},
        DebugInfo::kDefaultForTests);

    ABT optimized = std::move(translated);
    ASSERT_TRUE(phaseManager.optimize(optimized));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       agg_project_0\n"
        "|   RefBlock: \n"
        "|       Variable [agg_project_0]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: Centralized\n"
        "|   |       projections:\n"
        "|   RefBlock: \n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [agg_project_0]\n"
        "|           EvalPath []\n"
        "|           |   PathComposeM []\n"
        "|           |   |   PathField [c]\n"
        "|           |   |   PathConstant []\n"
        "|           |   |   Variable [c_agg_0]\n"
        "|           |   PathField [_id]\n"
        "|           |   PathConstant []\n"
        "|           |   Variable [groupByProj_0]\n"
        "|           Const [{}]\n"
        "GroupBy [global]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [groupByProj_0]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [c_agg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           Variable [preagg_0]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: HashPartitioning\n"
        "|   |       projections:\n"
        "|   |           groupByProj_0\n"
        "|   RefBlock: \n"
        "|       Variable [groupByProj_0]\n"
        "GroupBy [local]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [groupByProj_0]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [preagg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "PhysicalScan [{'<root>': 'scan_0', 'a': 'groupByProj_0'}, 'collection', parallel]\n"
        "    BindBlock:\n"
        "        [groupByProj_0]\n"
        "            Source []\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(ABTTranslate, UnwindBasic) {
    ABT translated = translatePipeline("[{$unwind: {path: '$a.b.c'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       embedProj_0\n"
        "|   RefBlock: \n"
        "|       Variable [embedProj_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [embedProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathField [b]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathField [c]\n"
        "|           |   PathConstant []\n"
        "|           |   Variable [unwoundProj_0]\n"
        "|           Variable [scan_0]\n"
        "Unwind []\n"
        "|   BindBlock:\n"
        "|       [unwoundPid_0]\n"
        "|           Source []\n"
        "|       [unwoundProj_0]\n"
        "|           Source []\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [unwoundProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathGet [b]\n"
        "|           |   PathGet [c]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, UnwindComplex) {
    ABT translated = translatePipeline(
        "[{$unwind: {path: '$a.b.c', includeArrayIndex: 'p1.pid', preserveNullAndEmptyArrays: "
        "true}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       embedPidProj_0\n"
        "|   RefBlock: \n"
        "|       Variable [embedPidProj_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [embedPidProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathField [p1]\n"
        "|           |   PathField [pid]\n"
        "|           |   PathConstant []\n"
        "|           |   If []\n"
        "|           |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   Variable [unwoundPid_0]\n"
        "|           |   |   |   Const [0]\n"
        "|           |   |   Variable [unwoundPid_0]\n"
        "|           |   Const [null]\n"
        "|           Variable [embedProj_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [embedProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathField [b]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathField [c]\n"
        "|           |   PathLambda []\n"
        "|           |   LambdaAbstraction [unwoundLambdaVarName_0]\n"
        "|           |   If []\n"
        "|           |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   Variable [unwoundPid_0]\n"
        "|           |   |   |   Const [0]\n"
        "|           |   |   Variable [unwoundProj_0]\n"
        "|           |   Variable [unwoundLambdaVarName_0]\n"
        "|           Variable [scan_0]\n"
        "Unwind [retainNonArrays]\n"
        "|   BindBlock:\n"
        "|       [unwoundPid_0]\n"
        "|           Source []\n"
        "|       [unwoundProj_0]\n"
        "|           Source []\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [unwoundProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathGet [b]\n"
        "|           |   PathGet [c]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, UnwindAndGroup) {
    ABT translated = translatePipeline(
        "[{$unwind:{path: '$a.b', preserveNullAndEmptyArrays: true}}, "
        "{$group:{_id: '$a.b'}}]");

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       agg_project_0\n"
        "|   RefBlock: \n"
        "|       Variable [agg_project_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [agg_project_0]\n"
        "|           EvalPath []\n"
        "|           |   PathField [_id]\n"
        "|           |   PathConstant []\n"
        "|           |   Variable [groupByProj_0]\n"
        "|           Const [{}]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [groupByProj_0]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|       RefBlock: \n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [groupByProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [embedProj_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [embedProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathField [a]\n"
        "|           |   PathTraverse []\n"
        "|           |   PathField [b]\n"
        "|           |   PathLambda []\n"
        "|           |   LambdaAbstraction [unwoundLambdaVarName_0]\n"
        "|           |   If []\n"
        "|           |   |   |   BinaryOp [Gte]\n"
        "|           |   |   |   |   Variable [unwoundPid_0]\n"
        "|           |   |   |   Const [0]\n"
        "|           |   |   Variable [unwoundProj_0]\n"
        "|           |   Variable [unwoundLambdaVarName_0]\n"
        "|           Variable [scan_0]\n"
        "Unwind [retainNonArrays]\n"
        "|   BindBlock:\n"
        "|       [unwoundPid_0]\n"
        "|           Source []\n"
        "|       [unwoundProj_0]\n"
        "|           Source []\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [unwoundProj_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathGet [b]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));
}

TEST(ABTTranslate, MatchIndex) {
    PrefixId prefixId;
    std::string scanDefName = "collection";
    ABT translated = translatePipeline("[{$match: {'a': 10}}]", scanDefName, prefixId);

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{scanDefName,
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        DebugInfo::kDefaultForTests);

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [10]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));

    ABT optimized = std::move(translated);
    ASSERT_TRUE(phaseManager.optimize(optimized));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'scan_0'}, 'collection']\n"
        "|   |   BindBlock:\n"
        "|   |       [scan_0]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'collection', indexDefName: 'index1', "
        "intervals: {['Const [10]', 'Const [10]']}]\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(ABTTranslate, MatchSortIndex) {
    PrefixId prefixId;
    std::string scanDefName = "collection";
    ABT translated =
        translatePipeline("[{$match: {'a': 10}}, {$sort: {'a': 1}}]", scanDefName, prefixId);

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{scanDefName,
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        DebugInfo::kDefaultForTests);

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       sort_0: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [sort_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [sort_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [10]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));

    ABT optimized = std::move(translated);
    ASSERT_TRUE(phaseManager.optimize(optimized));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       sort_0: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [sort_0]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [sort_0]\n"
        "|           EvalPath []\n"
        "|           |   PathGet [a]\n"
        "|           |   PathIdentity []\n"
        "|           Variable [scan_0]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'scan_0'}, 'collection']\n"
        "|   |   BindBlock:\n"
        "|   |       [scan_0]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'collection', indexDefName: 'index1', "
        "intervals: {['Const [10]', 'Const [10]']}]\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(ABTTranslate, RangeIndex) {
    PrefixId prefixId;
    std::string scanDefName = "collection";
    ABT translated =
        translatePipeline("[{$match: {'a': {$gt: 70, $lt: 90}}}]", scanDefName, prefixId);

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{scanDefName,
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        DebugInfo::kDefaultForTests);

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathComposeM []\n"
        "|   |   |   PathGet [a]\n"
        "|   |   |   PathTraverse []\n"
        "|   |   |   PathCompare [Gt] \n"
        "|   |   |   Const [70]\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Lt] \n"
        "|   |   Const [90]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));

    ABT optimized = std::move(translated);
    ASSERT_TRUE(phaseManager.optimize(optimized));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'scan_0'}, 'collection']\n"
        "|   |   BindBlock:\n"
        "|   |       [scan_0]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "HashJoin [joinType: Inner]\n"
        "|   |   Condition\n"
        "|   |       rid_0 = rid_1\n"
        "|   IndexScan [{'<rid>': 'rid_1'}, scanDefName: 'collection', indexDefName: 'index1', "
        "intervals: {('-Inf', 'Const [90]')}]\n"
        "|       BindBlock:\n"
        "|           [rid_1]\n"
        "|               Source []\n"
        "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'collection', indexDefName: 'index1', "
        "intervals: {('Const [70]', '+Inf')}]\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(ABTTranslate, Index1) {
    PrefixId prefixId;
    std::string scanDefName = "collection";
    ABT translated = translatePipeline("[{$match: {'a': 2, 'b': 2}}]", scanDefName, prefixId);

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       scan_0\n"
        "|   RefBlock: \n"
        "|       Variable [scan_0]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathComposeM []\n"
        "|   |   |   PathGet [a]\n"
        "|   |   |   PathTraverse []\n"
        "|   |   |   PathCompare [Eq] \n"
        "|   |   |   Const [2]\n"
        "|   |   PathGet [b]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [2]\n"
        "|   Variable [scan_0]\n"
        "Scan ['collection']\n"
        "    BindBlock:\n"
        "        [scan_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(translated));

    {
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{scanDefName,
               ScanDefinition{{},
                              {{"index1",
                                IndexDefinition{{{{{"a"}}, CollationOp::Ascending},
                                                 {{{"b"}}, CollationOp::Ascending}}}}}}}}},
            DebugInfo::kDefaultForTests);

        ABT optimized = translated;
        ASSERT_TRUE(phaseManager.optimize(optimized));

        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       scan_0\n"
            "|   RefBlock: \n"
            "|       Variable [scan_0]\n"
            "BinaryJoin [joinType: Inner, {rid_0}]\n"
            "|   |   Const [true]\n"
            "|   Seek [ridProjection: 'rid_0', {'<root>': 'scan_0'}, 'collection']\n"
            "|   |   BindBlock:\n"
            "|   |       [scan_0]\n"
            "|   |           Source []\n"
            "|   RefBlock: \n"
            "|       Variable [rid_0]\n"
            "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'collection', indexDefName: 'index1', "
            "intervals: {['Const [2]', 'Const [2]'], ['Const [2]', 'Const [2]']}]\n"
            "    BindBlock:\n"
            "        [rid_0]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }

    {
        // Demonstrate we can use an index over only one field.
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{scanDefName,
               ScanDefinition{
                   {}, {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
            DebugInfo::kDefaultForTests);

        ABT optimized = translated;
        ASSERT_TRUE(phaseManager.optimize(optimized));

        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       scan_0\n"
            "|   RefBlock: \n"
            "|       Variable [scan_0]\n"
            "Filter []\n"
            "|   EvalFilter []\n"
            "|   |   PathGet [b]\n"
            "|   |   PathTraverse []\n"
            "|   |   PathCompare [Eq] \n"
            "|   |   Const [2]\n"
            "|   Variable [scan_0]\n"
            "BinaryJoin [joinType: Inner, {rid_2}]\n"
            "|   |   Const [true]\n"
            "|   Seek [ridProjection: 'rid_2', {'<root>': 'scan_0'}, 'collection']\n"
            "|   |   BindBlock:\n"
            "|   |       [scan_0]\n"
            "|   |           Source []\n"
            "|   RefBlock: \n"
            "|       Variable [rid_2]\n"
            "IndexScan [{'<rid>': 'rid_2'}, scanDefName: 'collection', indexDefName: 'index1', "
            "intervals: {['Const [2]', 'Const [2]']}]\n"
            "    BindBlock:\n"
            "        [rid_2]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }
}

}  // namespace
}  // namespace mongo
