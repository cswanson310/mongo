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

#include <fstream>

#include "mongo/db/exec/sbe/abt/abt_lower.h"
#include "mongo/db/exec/sbe/abt/sbe_abt_test_util.h"
#include "mongo/db/pipeline/document_source_queue.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/unittest/temp_dir.h"

namespace mongo::optimizer {
namespace {

void createBSONTempFile(const std::string& tempFileName,
                        const std::vector<std::string>& jsonVector) {
    std::ofstream file(tempFileName, std::ios::out | std::ios::binary);
    for (const std::string& s : jsonVector) {
        const BSONObj& bsonObj = fromjson(s);
        file.write(bsonObj.objdata(), bsonObj.objsize());
    }
    file.close();
}

using ContextFn = std::function<ServiceContext::UniqueOperationContext()>;
using ResultSet = std::vector<BSONObj>;

ResultSet runSBEAST(const ContextFn& fn,
                    const std::string& pipelineStr,
                    const std::vector<std::string>& jsonVector) {
    PrefixId prefixId;

    unittest::TempDir tempDir("BSONTemp");

    const std::string& tempFileName = tempDir.path() + "/temp.bson";
    createBSONTempFile(tempFileName, jsonVector);

    constexpr size_t numberOfPartitions = 1;

    Metadata metadata({{"test",
                        {{{"type", "bson"}, {"fileName", tempFileName}},
                         {},
                         {(numberOfPartitions == 1) ? DistributionType::Centralized
                                                    : DistributionType::UnknownPartitioning}}}},
                      numberOfPartitions);
    ABT tree = translatePipeline(pipelineStr, "test", NamespaceString("bson"), prefixId);

    std::cerr << "********* Translated ABT *********\n";
    std::cerr << ExplainGenerator::explainV2(tree);
    std::cerr << "********* Translated ABT *********\n";

    OptPhaseManager phaseManager(
        OptPhaseManager::getAllRewritesSet(), prefixId, metadata, DebugInfo::kDefaultForTests);
    ASSERT_TRUE(phaseManager.optimize(tree));

    std::cerr << "********* Optimized ABT *********\n";
    std::cerr << ExplainGenerator::explainV2(tree);
    std::cerr << "********* Optimized ABT *********\n";

    SlotVarMap map;
    sbe::value::SlotIdGenerator ids;

    auto env = VariableEnvironment::build(tree);
    SBENodeLowering g{env, map, ids, phaseManager};
    auto sbePlan = g.optimize(tree);
    uassert(0, "Cannot optimize SBE plan", sbePlan != nullptr);

    sbe::CompileCtx ctx(std::make_unique<sbe::RuntimeEnvironment>());
    sbePlan->prepare(ctx);

    std::vector<sbe::value::SlotAccessor*> accessors;
    for (auto& [name, slot] : map) {
        accessors.emplace_back(sbePlan->getAccessor(ctx, slot));
    }
    // For now assert we only have one final projection.
    ASSERT_EQ(1, accessors.size());

    sbePlan->attachToOperationContext(fn().get());
    sbePlan->open(false);

    ResultSet results;
    while (sbePlan->getNext() != sbe::PlanState::IS_EOF) {
        if (results.size() > 1000) {
            uasserted(0, "Too many results!");
        }

        std::ostringstream os;
        os << accessors.at(0)->getViewOfValue();
        results.push_back(fromjson(os.str()));
    };
    sbePlan->close();

    return results;
}

ResultSet runPipeline(const ContextFn& fn,
                      const std::string& pipelineStr,
                      const std::vector<std::string>& jsonVector) {
    NamespaceString nss("test");
    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> pipeline =
        parsePipeline(pipelineStr, nss);

    const auto queueStage = DocumentSourceQueue::create(pipeline->getContext());
    for (const std::string& s : jsonVector) {
        BSONObj bsonObj = fromjson(s);
        queueStage->emplace_back(Document{bsonObj});
    }

    pipeline->addInitialSource(queueStage);

    boost::intrusive_ptr<ExpressionContext> expCtx;
    expCtx.reset(new ExpressionContext(fn().get(), nullptr, nss));

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> planExec =
        plan_executor_factory::make(expCtx, std::move(pipeline));

    ResultSet results;
    bool done = false;
    while (!done) {
        BSONObj outObj;
        auto result = planExec->getNext(&outObj, nullptr);
        switch (result) {
            case PlanExecutor::ADVANCED:
                results.push_back(outObj);
                break;
            case PlanExecutor::IS_EOF:
                done = true;
                break;
        }
    }

    return results;
}

bool compareBSONObj(const BSONObj& actual, const BSONObj& expected, const bool preserveFieldOrder) {
    BSONObj::ComparisonRulesSet rules = BSONObj::ComparisonRules::kConsiderFieldName;
    if (!preserveFieldOrder) {
        rules |= BSONObj::ComparisonRules::kIgnoreFieldOrder;
    }
    return actual.woCompare(expected, BSONObj(), rules) == 0;
}

bool compareResults(const ResultSet& expected,
                    const ResultSet& actual,
                    const bool preserveFieldOrder) {
    if (expected.size() != actual.size()) {
        std::cout << "Different result size: expected: " << expected.size()
                  << " vs actual: " << actual.size() << "\n";
        return false;
    }

    for (size_t i = 0; i < expected.size(); i++) {
        if (!compareBSONObj(actual.at(i), expected.at(i), preserveFieldOrder)) {
            std::cout << "Result at position " << i << "/" << expected.size()
                      << " mismatch: expected: " << expected.at(i) << " vs actual: " << actual.at(i)
                      << "\n";
            return false;
        }
    }

    return true;
}

bool compareSBEABTAgainstExpected(const ContextFn& fn,
                                  const std::string& pipelineStr,
                                  const std::vector<std::string>& jsonVector,
                                  const ResultSet& expected) {
    const ResultSet& actual = runSBEAST(fn, pipelineStr, jsonVector);
    return compareResults(expected, actual, true /*preserveFieldOrder*/);
}

bool comparePipelineAgainstExpected(const ContextFn& fn,
                                    const std::string& pipelineStr,
                                    const std::vector<std::string>& jsonVector,
                                    const ResultSet& expected) {
    const ResultSet& actual = runPipeline(fn, pipelineStr, jsonVector);
    return compareResults(expected, actual, true /*preserveFieldOrder*/);
}

bool compareSBEABTAgainstPipeline(const ContextFn& fn,
                                  const std::string& pipelineStr,
                                  const std::vector<std::string>& jsonVector,
                                  const bool preserveFieldOrder = true) {
    const ResultSet& pipelineResults = runPipeline(fn, pipelineStr, jsonVector);
    const ResultSet& sbeResults = runSBEAST(fn, pipelineStr, jsonVector);

    std::cout << "Pipeline: " << pipelineStr << ", input size: " << jsonVector.size() << "\n";
    const bool result = compareResults(pipelineResults, sbeResults, preserveFieldOrder);
    if (result) {
        std::cout << "Success. Result count: " << pipelineResults.size() << "\n";
        constexpr size_t maxResults = 1;
        for (size_t i = 0; i < std::min(pipelineResults.size(), maxResults); i++) {
            std::cout << "Result " << i << "/" << pipelineResults.size()
                      << ": actual: " << pipelineResults.at(i)
                      << " vs expected: " << sbeResults.at(i) << "\n";
        }
    }

    return result;
}

ResultSet toResultSet(const std::vector<std::string>& jsonVector) {
    ResultSet results;
    for (const std::string& jsonStr : jsonVector) {
        results.emplace_back(fromjson(jsonStr));
    }
    return results;
}

TEST_F(NodeSBE, DiffTestBasic) {
    const auto contextFn = [this]() { return makeOperationContext(); };
    const auto compare = [&contextFn](const std::string& pipelineStr,
                                      const std::vector<std::string>& jsonVector) {
        return compareSBEABTAgainstPipeline(
            contextFn, pipelineStr, jsonVector, true /*preserveFieldOrder*/);
    };

    ASSERT_TRUE(compareSBEABTAgainstExpected(
        contextFn, "[]", {"{a:1, b:2, c:3}"}, toResultSet({"{ a: 1, b: 2, c: 3 }"})));
    ASSERT_TRUE(compareSBEABTAgainstExpected(contextFn,
                                             "[{$addFields: {c: {$literal: 3}}}]",
                                             {"{a:1, b:2}"},
                                             toResultSet({"{ a: 1, b: 2, c: 3 }"})));

    ASSERT_TRUE(comparePipelineAgainstExpected(
        contextFn, "[]", {"{a:1, b:2, c:3}"}, toResultSet({"{ a: 1, b: 2, c: 3 }"})));
    ASSERT_TRUE(comparePipelineAgainstExpected(contextFn,
                                               "[{$addFields: {c: {$literal: 3}}}]",
                                               {"{a:1, b:2}"},
                                               toResultSet({"{ a: 1, b: 2, c: 3 }"})));

    ASSERT_TRUE(compare("[]", {"{a:1, b:2, c:3}"}));
    ASSERT_TRUE(compare("[{$addFields: {c: {$literal: 3}}}]", {"{a:1, b:2}"}));
}

TEST_F(NodeSBE, DiffTest) {
    const auto contextFn = [this]() { return makeOperationContext(); };
    const auto compare = [&contextFn](const std::string& pipelineStr,
                                      const std::vector<std::string>& jsonVector) {
        return compareSBEABTAgainstPipeline(
            contextFn, pipelineStr, jsonVector, true /*preserveFieldOrder*/);
    };

    // Consider checking if compare() works first.
    const auto compareUnordered = [&contextFn](const std::string& pipelineStr,
                                               const std::vector<std::string>& jsonVector) {
        return compareSBEABTAgainstPipeline(
            contextFn, pipelineStr, jsonVector, false /*preserveFieldOrder*/);
    };

    ASSERT_TRUE(compare("[]", {}));

    ASSERT_TRUE(compare("[{$project: {a: 1, b: 1}}]", {"{a: 10, b: 20, c: 30}"}));
    ASSERT_TRUE(compare("[{$match: {a: 2}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(compare("[{$match: {a: 5}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(compare("[{$match: {a: {$gte: 3}}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(compare("[{$match: {a: {$gte: 30}}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(
        compare("[{$match: {a: {$elemMatch: {$gte: 2, $lte: 3}}}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(
        compare("[{$match: {a: {$elemMatch: {$gte: 20, $lte: 30}}}}]", {"{a: [1, 2, 3, 4]}"}));

    ASSERT_TRUE(compare("[{$project: {'a.b': '$c'}}]", {"{a: {d: 1}, c: 2}"}));
    ASSERT_TRUE(compare("[{$project: {'a.b': '$c'}}]", {"{a: [{d: 1}, {d: 2}, {b: 10}], c: 2}"}));

    ASSERT_TRUE(compareUnordered("[{$project: {'a.b': '$c', c: 1}}]", {"{a: {d: 1}, c: 2}"}));
    ASSERT_TRUE(compareUnordered("[{$project: {'a.b': '$c', 'a.d': 1, c: 1}}]",
                                 {"{a: [{d: 1}, {d: 2}, {b: 10}], c: 2}"}));

    ASSERT_TRUE(
        compare("[{$project: {a: {$filter: {input: '$b', as: 'num', cond: {$and: [{$gte: ['$$num', "
                "2]}, {$lte: ['$$num', 3]}]}}}}}]",
                {"{b: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(
        compare("[{$project: {a: {$filter: {input: '$b', as: 'num', cond: {$and: [{$gte: ['$$num', "
                "3]}, {$lte: ['$$num', 2]}]}}}}}]",
                {"{b: [1, 2, 3, 4]}"}));

    ASSERT_TRUE(compare("[{$unwind: {path: '$a'}}]", {"{a: [1, 2, 3, 4]}"}));
    ASSERT_TRUE(compare("[{$unwind: {path: '$a.b'}}]", {"{a: {b: [1, 2, 3, 4]}}"}));

    ASSERT_TRUE(compare("[{$match:{'a.b.c':'aaa'}}]", {"{a: {b: {c: 'aaa'}}}"}));
    ASSERT_TRUE(
        compare("[{$match:{'a.b.c':'aaa'}}]", {"{a: {b: {c: 'aaa'}}}", "{a: {b: {c: 'aaa'}}}"}));
}

}  // namespace
}  // namespace mongo::optimizer
