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
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/unittest/unittest.h"

namespace mongo::optimizer {
namespace {

TEST(PhysOptimizer, PhysicalRewriterBasic) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("p1", "test");

    ABT projectionNode = make<EvaluationNode>(
        "p2", make<EvalPath>(make<PathIdentity>(), make<Variable>("p1")), std::move(scanNode));

    ABT filter1Node = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("p1")),
                                       std::move(projectionNode));

    ABT filter2Node = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("p2")),
                                       std::move(filter1Node));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"p2"}},
                                  std::move(filter2Node));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"test", {{}, {}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    {
        auto env = VariableEnvironment::build(optimized);
        ProjectionNameSet expSet = {"p1", "p2"};
        ASSERT_TRUE(expSet == env.topLevelProjections());
    }
    ASSERT_EQ(10, phaseManager.getPhysicalPlanExplorationCount());

    // Standard plan output.
    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       p2\n"
        "|   RefBlock: \n"
        "|       Variable [p2]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p2]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p2]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [p1]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p1]\n"
        "PhysicalScan [{'<root>': 'p1'}, 'test']\n"
        "    BindBlock:\n"
        "        [p1]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));

    {
        const MemoPhysicalNodeId id = phaseManager.getPhysicalNodeId();
        const Memo& physicalMemo = phaseManager.getMemo();

        // Plan output with properties.
        ASSERT_EQ(
            "Properties [cost: 3.1e+06]\n"
            "|   |   Logical:\n"
            "|   |       Logical cardinality estimate: 250\n"
            "|   |       Logical projection set: \n"
            "|   |           p1\n"
            "|   |           p2\n"
            "|   |       Collection availability: \n"
            "|   |           test\n"
            "|   |       Distribution availability: \n"
            "|   |               distribution:\n"
            "|   |                   type: Centralized\n"
            "|   |                   projections:\n"
            "|   Physical:\n"
            "|       distribution:\n"
            "|           type: Centralized\n"
            "|           projections:\n"
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       p2\n"
            "|   RefBlock: \n"
            "|       Variable [p2]\n"
            "Properties [cost: 3.1e+06]\n"
            "|   |   Logical:\n"
            "|   |       Logical cardinality estimate: 250\n"
            "|   |       Logical projection set: \n"
            "|   |           p1\n"
            "|   |           p2\n"
            "|   |       Indexing potential: [groupId: 0, scanProjection: p1]\n"
            "|   |       Collection availability: \n"
            "|   |           test\n"
            "|   |       Distribution availability: \n"
            "|   |               distribution:\n"
            "|   |                   type: Centralized\n"
            "|   |                   projections:\n"
            "|   Physical:\n"
            "|       projections:\n"
            "|           p2\n"
            "|       distribution:\n"
            "|           type: Centralized\n"
            "|           projections:\n"
            "|       Indexing requirement [Complete]: \n"
            "Filter []\n"
            "|   EvalFilter []\n"
            "|   |   PathIdentity []\n"
            "|   Variable [p2]\n"
            "Properties [cost: 2.6e+06]\n"
            "|   |   Logical:\n"
            "|   |       Logical cardinality estimate: 500\n"
            "|   |       Logical projection set: \n"
            "|   |           p1\n"
            "|   |           p2\n"
            "|   |       Indexing potential: [groupId: 0, scanProjection: p1]\n"
            "|   |       Collection availability: \n"
            "|   |           test\n"
            "|   |       Distribution availability: \n"
            "|   |               distribution:\n"
            "|   |                   type: Centralized\n"
            "|   |                   projections:\n"
            "|   Physical:\n"
            "|       projections:\n"
            "|           p2\n"
            "|       distribution:\n"
            "|           type: Centralized\n"
            "|           projections:\n"
            "|       Indexing requirement [Complete]: \n"
            "Evaluation []\n"
            "|   BindBlock:\n"
            "|       [p2]\n"
            "|           EvalPath []\n"
            "|           |   PathIdentity []\n"
            "|           Variable [p1]\n"
            "Properties [cost: 2.1e+06]\n"
            "|   |   Logical:\n"
            "|   |       Logical cardinality estimate: 500\n"
            "|   |       Logical projection set: \n"
            "|   |           p1\n"
            "|   |       Indexing potential: [groupId: 0, scanProjection: p1]\n"
            "|   |       Collection availability: \n"
            "|   |           test\n"
            "|   |       Distribution availability: \n"
            "|   |               distribution:\n"
            "|   |                   type: Centralized\n"
            "|   |                   projections:\n"
            "|   Physical:\n"
            "|       projections:\n"
            "|           p1\n"
            "|       distribution:\n"
            "|           type: Centralized\n"
            "|           projections:\n"
            "|       Indexing requirement [Complete]: \n"
            "Filter []\n"
            "|   EvalFilter []\n"
            "|   |   PathIdentity []\n"
            "|   Variable [p1]\n"
            "Properties [cost: 1.1e+06]\n"
            "|   |   Logical:\n"
            "|   |       Logical cardinality estimate: 1000\n"
            "|   |       Logical projection set: \n"
            "|   |           p1\n"
            "|   |       Indexing potential: [groupId: 0, scanProjection: p1]\n"
            "|   |       Collection availability: \n"
            "|   |           test\n"
            "|   |       Distribution availability: \n"
            "|   |               distribution:\n"
            "|   |                   type: Centralized\n"
            "|   |                   projections:\n"
            "|   Physical:\n"
            "|       projections:\n"
            "|           p1\n"
            "|       distribution:\n"
            "|           type: Centralized\n"
            "|           projections:\n"
            "|       Indexing requirement [Complete]: \n"
            "PhysicalScan [{'<root>': 'p1'}, 'test']\n"
            "    BindBlock:\n"
            "        [p1]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(make<MemoPhysicalDelegatorNode>(id),
                                        true /*displayPhysicalProperties*/,
                                        &physicalMemo));
    }
}

TEST(PhysOptimizer, GroupBy) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projectionANode = make<EvaluationNode>(
        "a", make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")), std::move(scanNode));
    ABT projectionBNode =
        make<EvaluationNode>("b",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(projectionANode));

    ABT groupByNode = make<GroupByNode>(ProjectionNameVector{"a"},
                                        ProjectionNameVector{"c"},
                                        ABTVector{make<Variable>("b")},
                                        std::move(projectionBNode));

    ABT filterCNode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("c")),
                                       std::move(groupByNode));

    ABT filterANode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("a")),
                                       std::move(filterCNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"c"}},
                                  std::move(filterANode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"test", {{}, {}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(17, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       c\n"
        "|   RefBlock: \n"
        "|       Variable [c]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [c]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [a]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [c]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           Variable [b]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [b]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [a]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "PhysicalScan [{'<root>': 'ptest'}, 'test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, GroupBy1) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projectionANode = make<EvaluationNode>("pa", Constant::null(), std::move(scanNode));
    ABT projectionA1Node =
        make<EvaluationNode>("pa1", Constant::null(), std::move(projectionANode));

    ABT groupByNode = make<GroupByNode>(ProjectionNameVector{},
                                        ProjectionNameVector{"pb", "pb1"},
                                        ABTVector{make<Variable>("pa"), make<Variable>("pa1")},
                                        std::move(projectionA1Node));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pb"}},
                                  std::move(groupByNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"test", {{}, {}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(8, phaseManager.getPhysicalPlanExplorationCount());

    // Projection "pb1" is unused and we do not generate an aggregation expression for it.
    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       pb\n"
        "|   RefBlock: \n"
        "|       Variable [pb]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [pb]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           Variable [pa]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [pa]\n"
        "|           Const [null]\n"
        "PhysicalScan [{}, 'test']\n"
        "    BindBlock:\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, Unwind) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projectionANode = make<EvaluationNode>(
        "a", make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")), std::move(scanNode));
    ABT projectionBNode =
        make<EvaluationNode>("b",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(projectionANode));

    ABT unwindNode =
        make<UnwindNode>("a", "a_pid", false /*retainNonArrays*/, std::move(projectionBNode));

    // This filter should stay above the unwind.
    ABT filterANode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("a")),
                                       std::move(unwindNode));

    // This filter should be pushed down below the unwind.
    ABT filterBNode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("b")),
                                       std::move(filterANode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"a", "b"}},
                                  std::move(filterBNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"test", {{}, {}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(17, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       a\n"
        "|   |       b\n"
        "|   RefBlock: \n"
        "|       Variable [a]\n"
        "|       Variable [b]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [a]\n"
        "Unwind []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           Source []\n"
        "|       [a_pid]\n"
        "|           Source []\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [b]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [b]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "PhysicalScan [{'<root>': 'ptest'}, 'test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, EvalCollation) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalNode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathIdentity>()), make<Variable>("root")),
        std::move(scanNode));

    ABT collationNode = make<CollationNode>(CollationRequirement({{"pa", CollationOp::Ascending}}),
                                            std::move(evalNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pa"}},
                                  std::move(collationNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1", {{}, {}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(6, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       pa\n"
        "|   RefBlock: \n"
        "|       Variable [pa]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       pa: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [pa]\n"
        "PhysicalScan [{'<root>': 'root', 'a': 'pa'}, 'c1']\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, FilterIndexing) {
    using namespace properties;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT filterNode = make<FilterNode>(
        make<EvalFilter>(
            make<PathGet>(
                "a", make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(1)))),
            make<Variable>("root")),
        std::move(scanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(filterNode));

    {
        PrefixId prefixId;
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
            prefixId,
            {{{"c1",
               ScanDefinition{
                   {}, {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
            {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

        // Demonstrate sargable node is rewritten from filter node.
        // Note: SargableNodes cannot be lowered and by default are not created unless we have
        // indexes.

        ABT optimized = rootNode;
        ASSERT_TRUE(phaseManager.optimize(optimized));

        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       root\n"
            "|   RefBlock: \n"
            "|       Variable [root]\n"
            "Sargable []\n"
            "|   |   |   |   Requirements map:\n"
            "|   |   |   |       ref projection: 'root', path: 'PathGet [a] PathTraverse [] "
            "PathIdentity []', intervals: {{{['Const [1]', 'Const [1]']}}}\n"
            "|   |   |   Candidate indexes\n"
            "|   |   |       candidate #1: index1, {}, {}, {{{['Const [1]', 'Const [1]']}}}\n"
            "|   |   BindBlock:\n"
            "|   RefBlock: \n"
            "|       Variable [root]\n"
            "Scan ['c1']\n"
            "    BindBlock:\n"
            "        [root]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }

    {
        PrefixId prefixId;
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{"c1",
               ScanDefinition{
                   {}, {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
            {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

        ABT optimized = rootNode;
        ASSERT_TRUE(phaseManager.optimize(optimized));
        ASSERT_EQ(5, phaseManager.getPhysicalPlanExplorationCount());

        // Test sargable filter is satisfied with an index scan.
        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       root\n"
            "|   RefBlock: \n"
            "|       Variable [root]\n"
            "BinaryJoin [joinType: Inner, {rid_0}]\n"
            "|   |   Const [true]\n"
            "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
            "|   |   BindBlock:\n"
            "|   |       [root]\n"
            "|   |           Source []\n"
            "|   RefBlock: \n"
            "|       Variable [rid_0]\n"
            "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'c1', indexDefName: 'index1', intervals: "
            "{['Const [1]', 'Const [1]']}]\n"
            "    BindBlock:\n"
            "        [rid_0]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }

    {
        PrefixId prefixId;
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{"c1", {{}, {}}}}},
            {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

        ABT optimized = rootNode;
        ASSERT_TRUE(phaseManager.optimize(optimized));
        ASSERT_EQ(4, phaseManager.getPhysicalPlanExplorationCount());

        // Test we can optimize sargable filter nodes even without an index.
        // (Imporant if we deleted the original filter node from the memo when creating the sargable
        // node).
        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       root\n"
            "|   RefBlock: \n"
            "|       Variable [root]\n"
            "Filter []\n"
            "|   EvalFilter []\n"
            "|   |   PathGet [a]\n"
            "|   |   PathTraverse []\n"
            "|   |   PathCompare [Eq] \n"
            "|   |   Const [1]\n"
            "|   Variable [root]\n"
            "PhysicalScan [{'<root>': 'root'}, 'c1']\n"
            "    BindBlock:\n"
            "        [root]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }
}

TEST(PhysOptimizer, CoveredScan) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalNode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(scanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pa"}},
                                  std::move(evalNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(4, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       pa\n"
        "|   RefBlock: \n"
        "|       Variable [pa]\n"
        "IndexScan [{'<indexKey> 0': 'pa'}, scanDefName: 'c1', indexDefName: 'index1', intervals: "
        "{('-Inf', '+Inf')}]\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, EvalIndexing) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalNode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(scanNode));

    ABT filterNode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Gt, Constant::int64(1)),
                                          make<Variable>("pa")),
                         std::move(evalNode));

    ABT collationNode = make<CollationNode>(CollationRequirement({{"pa", CollationOp::Ascending}}),
                                            std::move(filterNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pa"}},
                                  std::move(collationNode));

    {
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{"c1",
               ScanDefinition{
                   {}, {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
            {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

        ABT optimized = rootNode;
        ASSERT_TRUE(phaseManager.optimize(optimized));
        ASSERT_EQ(6, phaseManager.getPhysicalPlanExplorationCount());

        // Should not need a collation node.
        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       pa\n"
            "|   RefBlock: \n"
            "|       Variable [pa]\n"
            "IndexScan [{'<indexKey> 0': 'pa'}, scanDefName: 'c1', indexDefName: 'index1', "
            "intervals: {('Const [1]', '+Inf')}]\n"
            "    BindBlock:\n"
            "        [pa]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }

    {
        // Index and collation node have incompatible ops.
        OptPhaseManager phaseManager(
            {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
             OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
            prefixId,
            {{{"c1",
               ScanDefinition{
                   {}, {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Clustered}}}}}}}}},
            {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

        ABT optimized = rootNode;
        ASSERT_TRUE(phaseManager.optimize(optimized));
        ASSERT_EQ(11, phaseManager.getPhysicalPlanExplorationCount());

        // Index does not have the right collation and now we need a collation node.
        ASSERT_EQ(
            "RootNode []\n"
            "|   |   projections:\n"
            "|   |       pa\n"
            "|   RefBlock: \n"
            "|       Variable [pa]\n"
            "CollationNode []\n"
            "|   |   collation:\n"
            "|   |       pa: Ascending\n"
            "|   RefBlock: \n"
            "|       Variable [pa]\n"
            "IndexScan [{'<indexKey> 0': 'pa'}, scanDefName: 'c1', indexDefName: 'index1', "
            "intervals: {('Const [1]', '+Inf')}]\n"
            "    BindBlock:\n"
            "        [pa]\n"
            "            Source []\n",
            ExplainGenerator::explainV2(optimized));
    }
}

TEST(PhysOptimizer, EvalIndexing1) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalNode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(scanNode));

    ABT filterNode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(1)),
                                          make<Variable>("pa")),
                         std::move(evalNode));

    ABT collationNode = make<CollationNode>(CollationRequirement({{"pa", CollationOp::Ascending}}),
                                            std::move(filterNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(collationNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(7, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
        "|   |   BindBlock:\n"
        "|   |       [root]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "IndexScan [{'<indexKey> 0': 'pa', '<rid>': 'rid_0'}, scanDefName: 'c1', indexDefName: "
        "'index1', intervals: {['Const [1]', 'Const [1]']}]\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, MultiKeyIndex) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalANode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(scanNode));

    ABT filterANode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(1)),
                                          make<Variable>("pa")),
                         std::move(evalANode));

    ABT evalBNode = make<EvaluationNode>(
        "pb",
        make<EvalPath>(make<PathGet>("b", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(filterANode));

    ABT filterBNode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Gt, Constant::int64(2)),
                                          make<Variable>("pb")),
                         std::move(evalBNode));

    ABT collationNode = make<CollationNode>(
        CollationRequirement({{"pa", CollationOp::Ascending}, {"pb", CollationOp::Ascending}}),
        std::move(filterBNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(collationNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}},
                           {"index2", IndexDefinition{{{{{"b"}}, CollationOp::Descending}}}}}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(22, phaseManager.getPhysicalPlanExplorationCount());

    // Index2 will be used in reverse direction.
    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
        "|   |   BindBlock:\n"
        "|   |       [root]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "HashJoin [joinType: Inner]\n"
        "|   |   Condition\n"
        "|   |       rid_0 = rid_1\n"
        "|   IndexScan [{'<indexKey> 0': 'pb', '<rid>': 'rid_1'}, scanDefName: 'c1', indexDefName: "
        "'index2', intervals: {('Const [2]', '+Inf')}, reversed.]\n"
        "|       BindBlock:\n"
        "|           [pb]\n"
        "|               Source []\n"
        "|           [rid_1]\n"
        "|               Source []\n"
        "IndexScan [{'<indexKey> 0': 'pa', '<rid>': 'rid_0'}, scanDefName: 'c1', indexDefName: "
        "'index1', intervals: {['Const [1]', 'Const [1]']}]\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, CompoundIndex1) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalANode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(scanNode));

    ABT filterANode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(1)),
                                          make<Variable>("pa")),
                         std::move(evalANode));

    ABT evalBNode = make<EvaluationNode>(
        "pb",
        make<EvalPath>(make<PathGet>("b", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(filterANode));

    ABT filterBNode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(2)),
                                          make<Variable>("pb")),
                         std::move(evalBNode));

    ABT evalCNode = make<EvaluationNode>(
        "pc",
        make<EvalPath>(make<PathGet>("c", make<PathTraverse>(make<PathIdentity>())),
                       make<Variable>("root")),
        std::move(filterBNode));

    ABT filterCNode =
        make<FilterNode>(make<EvalFilter>(make<PathCompare>(Operations::Eq, Constant::int64(3)),
                                          make<Variable>("pc")),
                         std::move(evalCNode));

    ABT collationNode = make<CollationNode>(
        CollationRequirement({{"pa", CollationOp::Ascending}, {"pb", CollationOp::Ascending}}),
        std::move(filterCNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(collationNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1",
                            IndexDefinition{{{{{"a"}}, CollationOp::Ascending},
                                             {{{"c"}}, CollationOp::Descending}}}},
                           {"index2", IndexDefinition{{{{{"b"}}, CollationOp::Ascending}}}}}}}}},

        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(46, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
        "|   |   BindBlock:\n"
        "|   |       [root]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "HashJoin [joinType: Inner]\n"
        "|   |   Condition\n"
        "|   |       rid_0 = rid_1\n"
        "|   IndexScan [{'<indexKey> 0': 'pb', '<rid>': 'rid_1'}, scanDefName: 'c1', indexDefName: "
        "'index2', intervals: {['Const [2]', 'Const [2]']}]\n"
        "|       BindBlock:\n"
        "|           [pb]\n"
        "|               Source []\n"
        "|           [rid_1]\n"
        "|               Source []\n"
        "IndexScan [{'<indexKey> 0': 'pa', '<indexKey> 1': 'pc', '<rid>': 'rid_0'}, scanDefName: "
        "'c1', indexDefName: 'index1', intervals: {['Const [1]', 'Const [1]'], ['Const [3]', "
        "'Const [3]']}]\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n"
        "        [pc]\n"
        "            Source []\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, IndexBoundsIntersect) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT filterNode = make<FilterNode>(
        make<EvalFilter>(
            make<PathComposeM>(make<PathGet>("a",
                                             make<PathTraverse>(make<PathCompare>(
                                                 Operations::Gt, Constant::int64(70)))),
                               make<PathGet>("a",
                                             make<PathTraverse>(make<PathCompare>(
                                                 Operations::Lt, Constant::int64(90))))),
            make<Variable>("root")),
        std::move(scanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(filterNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1", IndexDefinition{{{{{"a"}}, CollationOp::Ascending}}}}}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(7, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
        "|   |   BindBlock:\n"
        "|   |       [root]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "HashJoin [joinType: Inner]\n"
        "|   |   Condition\n"
        "|   |       rid_0 = rid_1\n"
        "|   IndexScan [{'<rid>': 'rid_1'}, scanDefName: 'c1', indexDefName: 'index1', intervals: "
        "{('-Inf', 'Const [90]')}]\n"
        "|       BindBlock:\n"
        "|           [rid_1]\n"
        "|               Source []\n"
        "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'c1', indexDefName: 'index1', intervals: "
        "{('Const [70]', '+Inf')}]\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, ParallelScan) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT filterNode = make<FilterNode>(
        make<EvalFilter>(
            make<PathGet>(
                "a", make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(1)))),
            make<Variable>("root")),
        std::move(scanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(filterNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1", ScanDefinition{{}, {}, {DistributionType::UnknownPartitioning}}}},
         5 /*numberOfPartitions*/},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(8, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: Centralized\n"
        "|   |       projections:\n"
        "|   RefBlock: \n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [1]\n"
        "|   Variable [root]\n"
        "PhysicalScan [{'<root>': 'root'}, 'c1', parallel]\n"
        "    BindBlock:\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, LocalGlobalAgg) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalANode = make<EvaluationNode>(
        "pa",
        make<EvalPath>(make<PathGet>("a", make<PathIdentity>()), make<Variable>("root")),
        std::move(scanNode));
    ABT evalBNode = make<EvaluationNode>(
        "pb",
        make<EvalPath>(make<PathGet>("b", make<PathIdentity>()), make<Variable>("root")),
        std::move(evalANode));

    ABT groupByNode =
        make<GroupByNode>(ProjectionNameVector{"pa"},
                          ProjectionNameVector{"pc"},
                          ABTVector{make<FunctionCall>("$sum", ABTVector{make<Variable>("pb")})},
                          std::move(evalBNode));

    ABT rootNode =
        make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pa", "pc"}},
                       std::move(groupByNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1", ScanDefinition{{}, {}, {DistributionType::UnknownPartitioning}}}},
         5 /*numberOfPartitions*/},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(35, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       pa\n"
        "|   |       pc\n"
        "|   RefBlock: \n"
        "|       Variable [pa]\n"
        "|       Variable [pc]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: Centralized\n"
        "|   |       projections:\n"
        "|   RefBlock: \n"
        "GroupBy [global]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [pa]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [pc]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           Variable [preagg_0]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: HashPartitioning\n"
        "|   |       projections:\n"
        "|   |           pa\n"
        "|   RefBlock: \n"
        "|       Variable [pa]\n"
        "GroupBy [local]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [pa]\n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [preagg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           Variable [pb]\n"
        "PhysicalScan [{'<root>': 'root', 'a': 'pa', 'b': 'pb'}, 'c1', parallel]\n"
        "    BindBlock:\n"
        "        [pa]\n"
        "            Source []\n"
        "        [pb]\n"
        "            Source []\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, LocalGlobalAgg1) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT evalBNode = make<EvaluationNode>(
        "pb",
        make<EvalPath>(make<PathGet>("b", make<PathIdentity>()), make<Variable>("root")),
        std::move(scanNode));

    ABT groupByNode =
        make<GroupByNode>(ProjectionNameVector{},
                          ProjectionNameVector{"pc"},
                          ABTVector{make<FunctionCall>("$sum", ABTVector{make<Variable>("pb")})},
                          std::move(evalBNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"pc"}},
                                  std::move(groupByNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1", ScanDefinition{{}, {}, {DistributionType::UnknownPartitioning}}}},
         5 /*numberOfPartitions*/},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(19, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       pc\n"
        "|   RefBlock: \n"
        "|       Variable [pc]\n"
        "GroupBy [global]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [pc]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           Variable [preagg_0]\n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: Centralized\n"
        "|   |       projections:\n"
        "|   RefBlock: \n"
        "GroupBy [local]\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   aggregations:\n"
        "|       BindBlock:\n"
        "|           [preagg_0]\n"
        "|               Source []\n"
        "|       RefBlock: \n"
        "|           FunctionCall [$sum]\n"
        "|           Variable [pb]\n"
        "PhysicalScan [{'<root>': 'root', 'b': 'pb'}, 'c1', parallel]\n"
        "    BindBlock:\n"
        "        [pb]\n"
        "            Source []\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, LocalLimitSkip) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT limitSkipNode =
        make<LimitSkipNode>(properties::LimitSkipRequirement{20, 10}, std::move(scanNode));
    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(limitSkipNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1", ScanDefinition{{}, {}, {DistributionType::UnknownPartitioning}}}},
         5 /*numberOfPartitions*/},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(20, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "Properties [cost: 59000]\n"
        "|   |   Logical:\n"
        "|   |       Logical cardinality estimate: 20\n"
        "|   |       Logical projection set: \n"
        "|   |           root\n"
        "|   |       Collection availability: \n"
        "|   |           c1\n"
        "|   |       Distribution availability: \n"
        "|   |               distribution:\n"
        "|   |                   type: Centralized\n"
        "|   |                   projections:\n"
        "|   |               distribution:\n"
        "|   |                   type: Replicated\n"
        "|   |                   projections:\n"
        "|   |               distribution:\n"
        "|   |                   type: UnknownPartitioning\n"
        "|   |                   projections:\n"
        "|   Physical:\n"
        "|       distribution:\n"
        "|           type: Centralized\n"
        "|           projections:\n"
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "Properties [cost: 59000]\n"
        "|   |   Logical:\n"
        "|   |       Logical cardinality estimate: 1000\n"
        "|   |       Logical projection set: \n"
        "|   |           root\n"
        "|   |       Indexing potential: [groupId: 0, scanProjection: root]\n"
        "|   |       Collection availability: \n"
        "|   |           c1\n"
        "|   |       Distribution availability: \n"
        "|   |               distribution:\n"
        "|   |                   type: UnknownPartitioning\n"
        "|   |                   projections:\n"
        "|   Physical:\n"
        "|       limitSkip: []\n"
        "|           limit: 20\n"
        "|           skip: 10\n"
        "|       projections:\n"
        "|           root\n"
        "|       distribution:\n"
        "|           type: Centralized\n"
        "|           projections:\n"
        "|       Indexing requirement [Complete]: \n"
        "LimitSkipNode []\n"
        "|   limitSkip: []\n"
        "|       limit: 20\n"
        "|       skip: 10\n"
        "Properties [cost: 29000]\n"
        "|   |   Logical:\n"
        "|   |       Logical cardinality estimate: 1000\n"
        "|   |       Logical projection set: \n"
        "|   |           root\n"
        "|   |       Indexing potential: [groupId: 0, scanProjection: root]\n"
        "|   |       Collection availability: \n"
        "|   |           c1\n"
        "|   |       Distribution availability: \n"
        "|   |               distribution:\n"
        "|   |                   type: UnknownPartitioning\n"
        "|   |                   projections:\n"
        "|   Physical:\n"
        "|       limitSkip: [enforced]\n"
        "|           limit: 20\n"
        "|           skip: 10\n"
        "|       projections:\n"
        "|           root\n"
        "|       distribution:\n"
        "|           type: Centralized\n"
        "|           projections:\n"
        "|       Indexing requirement [Complete]: \n"
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: Centralized\n"
        "|   |       projections:\n"
        "|   RefBlock: \n"
        "Properties [cost: 26000]\n"
        "|   |   Logical:\n"
        "|   |       Logical cardinality estimate: 1000\n"
        "|   |       Logical projection set: \n"
        "|   |           root\n"
        "|   |       Indexing potential: [groupId: 0, scanProjection: root]\n"
        "|   |       Collection availability: \n"
        "|   |           c1\n"
        "|   |       Distribution availability: \n"
        "|   |               distribution:\n"
        "|   |                   type: UnknownPartitioning\n"
        "|   |                   projections:\n"
        "|   Physical:\n"
        "|       limitSkip: [enforced]\n"
        "|           limit: 20\n"
        "|           skip: 10\n"
        "|       projections:\n"
        "|           root\n"
        "|       distribution:\n"
        "|           type: UnknownPartitioning\n"
        "|           projections:\n"
        "|       Indexing requirement [Complete]: \n"
        "PhysicalScan [{'<root>': 'root'}, 'c1', parallel]\n"
        "    BindBlock:\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(
            make<MemoPhysicalDelegatorNode>(phaseManager.getPhysicalNodeId()),
            true /*displayPhysicalProperties*/,
            &phaseManager.getMemo()));
}

TEST(PhysOptimizer, PartialIndex) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT filterANode = make<FilterNode>(
        make<EvalFilter>(
            make<PathGet>(
                "a", make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(3)))),
            make<Variable>("root")),
        std::move(scanNode));
    ABT filterBNode = make<FilterNode>(
        make<EvalFilter>(
            make<PathGet>(
                "b", make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(2)))),
            make<Variable>("root")),
        std::move(filterANode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(filterBNode));

    // TODO: test cases where partial filter bound is a range which subsumes the query requirement.
    auto conversionResult = convertExprToPartialSchemaReq(make<EvalFilter>(
        make<PathGet>("b",
                      make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(2)))),
        make<Variable>("root")));
    ASSERT_TRUE(conversionResult._success);

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        {{{"c1",
           ScanDefinition{{},
                          {{"index1",
                            IndexDefinition{{{{{"a"}}, CollationOp::Ascending}},
                                            2 /*version*/,
                                            0 /*orderingBits*/,
                                            {DistributionType::Centralized},
                                            std::move(conversionResult._reqMap)}}}}}}},
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(6, phaseManager.getPhysicalPlanExplorationCount());

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "BinaryJoin [joinType: Inner, {rid_0}]\n"
        "|   |   Const [true]\n"
        "|   Seek [ridProjection: 'rid_0', {'<root>': 'root'}, 'c1']\n"
        "|   |   BindBlock:\n"
        "|   |       [root]\n"
        "|   |           Source []\n"
        "|   RefBlock: \n"
        "|       Variable [rid_0]\n"
        "IndexScan [{'<rid>': 'rid_0'}, scanDefName: 'c1', indexDefName: 'index1', intervals: "
        "{['Const [3]', 'Const [3]']}]\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

TEST(PhysOptimizer, RequireRID) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("root", "c1");

    ABT filterNode = make<FilterNode>(
        make<EvalFilter>(
            make<PathGet>(
                "a", make<PathTraverse>(make<PathCompare>(Operations::Eq, Constant::int64(3)))),
            make<Variable>("root")),
        std::move(scanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"root"}},
                                  std::move(filterNode));

    OptPhaseManager phaseManager(
        {OptPhaseManager::OptPhase::MemoLogicalRewritePhase,
         OptPhaseManager::OptPhase::MemoPhysicalRewritePhase},
        prefixId,
        true /*requireRID*/,
        {{{"c1", ScanDefinition{{}, {}}}}},
        heuristicCE,
        {true /*debugMode*/, 2 /*debugLevel*/, DebugInfo::kIterationLimitForTests});

    ABT optimized = rootNode;
    ASSERT_TRUE(phaseManager.optimize(optimized));
    ASSERT_EQ(3, phaseManager.getPhysicalPlanExplorationCount());

    // Make sure the Scan node returns rid.
    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       root\n"
        "|   RefBlock: \n"
        "|       Variable [root]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathGet [a]\n"
        "|   |   PathTraverse []\n"
        "|   |   PathCompare [Eq] \n"
        "|   |   Const [3]\n"
        "|   Variable [root]\n"
        "PhysicalScan [{'<rid>': 'rid_0', '<root>': 'root'}, 'c1']\n"
        "    BindBlock:\n"
        "        [rid_0]\n"
        "            Source []\n"
        "        [root]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(optimized));
}

}  // namespace
}  // namespace mongo::optimizer
