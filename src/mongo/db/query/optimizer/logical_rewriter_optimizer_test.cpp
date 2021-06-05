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
#include "mongo/db/query/optimizer/cascades/logical_props_derivation.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/unittest/unittest.h"

namespace mongo::optimizer {
namespace {

TEST(LogicalOptimizer, RootNodeMerge) {
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("a", "test");
    ABT limitSkipNode1 =
        make<LimitSkipNode>(properties::LimitSkipRequirement(-1, 10), std::move(scanNode));
    ABT limitSkipNode2 =
        make<LimitSkipNode>(properties::LimitSkipRequirement(5, 0), std::move(limitSkipNode1));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"a"}},
                                  std::move(limitSkipNode2));

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "    a\n"
        "  RefBlock: \n"
        "    Variable [a]\n"
        "  LimitSkipNode []\n"
        "    limitSkip: []\n"
        "      limit: 5\n"
        "      skip: 0\n"
        "    LimitSkipNode []\n"
        "      limitSkip: []\n"
        "        limit: (none)\n"
        "        skip: 10\n"
        "      Scan ['test']\n"
        "        BindBlock:\n"
        "          [a]\n"
        "            Source []\n",
        ExplainGenerator::explain(rootNode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT rewritten = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(rewritten));

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "    a\n"
        "  RefBlock: \n"
        "    Variable [a]\n"
        "  LimitSkipNode []\n"
        "    limitSkip: []\n"
        "      limit: 5\n"
        "      skip: 10\n"
        "    Scan ['test']\n"
        "      BindBlock:\n"
        "        [a]\n"
        "          Source []\n",
        ExplainGenerator::explain(rewritten));
}

TEST(LogicalOptimizer, Memo) {
    using namespace cascades;
    using namespace properties;

    Metadata metadata{{{"test", {}}}};
    Memo memo(
        DebugInfo::kDefaultForTests,
        std::bind(
            logicalPropsDerive, std::placeholders::_1, std::cref(metadata), std::placeholders::_2),
        heuristicCE);

    ABT scanNode = make<ScanNode>("ptest", "test");
    ABT filterNode = make<FilterNode>(
        make<EvalFilter>(make<PathConstant>(make<UnaryOp>(Operations::Neg, Constant::int64(1))),
                         make<Variable>("ptest")),
        std::move(scanNode));
    ABT evalNode = make<EvaluationNode>(
        "P1",
        make<EvalPath>(make<PathConstant>(Constant::int64(2)), make<Variable>("ptest")),
        std::move(filterNode));

    Memo::NodeIdSet insertedNodeIds;
    const GroupIdType rootGroupId = memo.integrate(evalNode, {}, insertedNodeIds);
    ASSERT_EQ(2, rootGroupId);
    ASSERT_EQ(3, memo.getGroupCount());

    Memo::NodeIdSet expectedInsertedNodeIds = {{0, 0}, {1, 0}, {2, 0}};
    ASSERT_TRUE(insertedNodeIds == expectedInsertedNodeIds);

    ASSERT_EQ(
        "Memo\n"
        "    Group #0\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 1000\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Scan ['test']\n"
        "                    BindBlock:\n"
        "                        [ptest]\n"
        "                            Source []\n"
        "        Physical nodes\n"
        "    Group #1\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 500\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   EvalFilter []\n"
        "                |   |   PathConstant []\n"
        "                |   |   UnaryOp [Neg]\n"
        "                |   |   Const [1]\n"
        "                |   Variable [ptest]\n"
        "                MemoLogicalDelegator [groupId: 0]\n"
        "        Physical nodes\n"
        "    Group #2\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 500\n"
        "            Logical projection set: \n"
        "                P1\n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Evaluation []\n"
        "                |   BindBlock:\n"
        "                |       [P1]\n"
        "                |           EvalPath []\n"
        "                |           |   PathConstant []\n"
        "                |           |   Const [2]\n"
        "                |           Variable [ptest]\n"
        "                MemoLogicalDelegator [groupId: 1]\n"
        "        Physical nodes\n",
        ExplainGenerator::explainMemo(memo));

    {
        // Try to insert into the memo again.
        Memo::NodeIdSet insertedNodeIds;
        const GroupIdType group = memo.integrate(evalNode, {}, insertedNodeIds);
        ASSERT_EQ(2, group);
        ASSERT_EQ(3, memo.getGroupCount());

        // Nothing was inserted.
        ASSERT_EQ(1, memo.getGroup(0)._logicalNodes.size());
        ASSERT_EQ(1, memo.getGroup(1)._logicalNodes.size());
        ASSERT_EQ(1, memo.getGroup(2)._logicalNodes.size());
    }

    // Insert a different tree, this time only scan and project.
    ABT scanNode1 = make<ScanNode>("ptest", "test");
    ABT evalNode1 = make<EvaluationNode>(
        "P1",
        make<EvalPath>(make<PathConstant>(Constant::int64(2)), make<Variable>("ptest")),
        std::move(scanNode1));

    {
        Memo::NodeIdSet insertedNodeIds1;
        const GroupIdType rootGroupId1 = memo.integrate(evalNode1, {}, insertedNodeIds1);
        ASSERT_EQ(3, rootGroupId1);
        ASSERT_EQ(4, memo.getGroupCount());

        // Nothing was inserted in first 3 groups.
        ASSERT_EQ(1, memo.getGroup(0)._logicalNodes.size());
        ASSERT_EQ(1, memo.getGroup(1)._logicalNodes.size());
        ASSERT_EQ(1, memo.getGroup(2)._logicalNodes.size());
    }

    {
        const Group& group = memo.getGroup(3);
        ASSERT_EQ(1, group._logicalNodes.size());

        ASSERT_EQ(
            "Evaluation []\n"
            "  BindBlock:\n"
            "    [P1]\n"
            "      EvalPath []\n"
            "        PathConstant []\n"
            "          Const [2]\n"
            "        Variable [ptest]\n"
            "  MemoLogicalDelegator [groupId: 0]\n",
            ExplainGenerator::explain(group._logicalNodes.at(0)));
    }
}

TEST(Optimizer, FilterFilterSmallRewrite) {
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");
    ABT filter1Node = make<FilterNode>(Constant::int64(1), std::move(scanNode));
    ABT filter2Node = make<FilterNode>(Constant::int64(2), std::move(filter1Node));

    ASSERT_EQ(
        "Filter []\n"
        "  Const [2]\n"
        "  Filter []\n"
        "    Const [1]\n"
        "    Scan ['test']\n"
        "      BindBlock:\n"
        "        [ptest]\n"
        "          Source []\n",
        ExplainGenerator::explain(filter2Node));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(filter2Node);
    ASSERT_TRUE(phaseManager.optimize(latest));

    ASSERT_EQ(
        "Filter []\n"
        "  Const [1]\n"
        "  Filter []\n"
        "    Const [2]\n"
        "    Scan ['test']\n"
        "      BindBlock:\n"
        "        [ptest]\n"
        "          Source []\n",
        ExplainGenerator::explain(latest));
}

TEST(LogicalOptimizer, FilterFilterRewrite) {
    using namespace cascades;

    ABT scanNode = make<ScanNode>("ptest", "test");
    ABT filter1Node = make<FilterNode>(Constant::int64(1), std::move(scanNode));
    ABT filter2Node = make<FilterNode>(Constant::int64(2), std::move(filter1Node));
    ABT filter3Node = make<FilterNode>(Constant::int64(3), std::move(filter2Node));

    ASSERT_EQ(
        "Filter []\n"
        "  Const [3]\n"
        "  Filter []\n"
        "    Const [2]\n"
        "    Filter []\n"
        "      Const [1]\n"
        "      Scan ['test']\n"
        "        BindBlock:\n"
        "          [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explain(filter3Node));

    Metadata metadata{{{"test", {}}}};
    Memo memo(
        DebugInfo::kDefaultForTests,
        std::bind(
            logicalPropsDerive, std::placeholders::_1, std::cref(metadata), std::placeholders::_2),
        heuristicCE);

    PrefixId prefixId;
    LogicalRewriter rewriter(memo, prefixId, metadata, LogicalRewriter::getRewriteSet());
    const GroupIdType rootGroupId = rewriter.addInitialNode(filter3Node);
    rewriter.rewriteToFixPoint();
    ABT latest = rewriter.getLatestPlan(rootGroupId);

    // Note: this assert depends on the order on which we consider rewrites.
    ASSERT_EQ(
        "Filter []\n"
        "  Const [1]\n"
        "  Filter []\n"
        "    Const [3]\n"
        "    Filter []\n"
        "      Const [2]\n"
        "      Scan ['test']\n"
        "        BindBlock:\n"
        "          [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explain(latest));

    // Note: this assert depends on the order on which we consider rewrites.
    ASSERT_EQ(
        "Memo\n"
        "    Group #0\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 1000\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Scan ['test']\n"
        "                    BindBlock:\n"
        "                        [ptest]\n"
        "                            Source []\n"
        "        Physical nodes\n"
        "    Group #1\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 500\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [1]\n"
        "                MemoLogicalDelegator [groupId: 0]\n"
        "        Physical nodes\n"
        "    Group #2\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 250\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [2]\n"
        "                MemoLogicalDelegator [groupId: 1]\n"
        "            LogicalNode #1\n"
        "                Filter []\n"
        "                |   Const [1]\n"
        "                MemoLogicalDelegator [groupId: 7]\n"
        "        Physical nodes\n"
        "    Group #3\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 125\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [3]\n"
        "                MemoLogicalDelegator [groupId: 2]\n"
        "            LogicalNode #1\n"
        "                Filter []\n"
        "                |   Const [2]\n"
        "                MemoLogicalDelegator [groupId: 4]\n"
        "            LogicalNode #2\n"
        "                Filter []\n"
        "                |   Const [1]\n"
        "                MemoLogicalDelegator [groupId: 6]\n"
        "        Physical nodes\n"
        "    Group #4\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 250\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [3]\n"
        "                MemoLogicalDelegator [groupId: 1]\n"
        "            LogicalNode #1\n"
        "                Filter []\n"
        "                |   Const [1]\n"
        "                MemoLogicalDelegator [groupId: 5]\n"
        "        Physical nodes\n"
        "    Group #5\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 500\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [3]\n"
        "                MemoLogicalDelegator [groupId: 0]\n"
        "        Physical nodes\n"
        "    Group #6\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 250\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [2]\n"
        "                MemoLogicalDelegator [groupId: 5]\n"
        "            LogicalNode #1\n"
        "                Filter []\n"
        "                |   Const [3]\n"
        "                MemoLogicalDelegator [groupId: 7]\n"
        "        Physical nodes\n"
        "    Group #7\n"
        "        Logical properties:\n"
        "            Logical cardinality estimate: 500\n"
        "            Logical projection set: \n"
        "                ptest\n"
        "            Indexing potential: [groupId: 0, scanProjection: ptest]\n"
        "            Collection availability: \n"
        "                test\n"
        "            Distribution availability: \n"
        "                    distribution:\n"
        "                        type: Centralized\n"
        "                        projections:\n"
        "        Logical nodes\n"
        "            LogicalNode #0\n"
        "                Filter []\n"
        "                |   Const [2]\n"
        "                MemoLogicalDelegator [groupId: 0]\n"
        "        Physical nodes\n",
        ExplainGenerator::explainMemo(memo));
}

TEST(LogicalOptimizer, FilterProjectRewrite) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");
    ABT collationNode = make<CollationNode>(
        CollationRequirement({{"ptest", CollationOp::Ascending}}), std::move(scanNode));
    ABT evalNode =
        make<EvaluationNode>("P1",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(collationNode));
    ABT filterNode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("P1")),
                                      std::move(evalNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{{}}, std::move(filterNode));

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "  RefBlock: \n"
        "  Filter []\n"
        "    EvalFilter []\n"
        "      PathIdentity []\n"
        "      Variable [P1]\n"
        "    Evaluation []\n"
        "      BindBlock:\n"
        "        [P1]\n"
        "          EvalPath []\n"
        "            PathIdentity []\n"
        "            Variable [ptest]\n"
        "      CollationNode []\n"
        "        collation:\n"
        "          ptest: Ascending\n"
        "        RefBlock: \n"
        "          Variable [ptest]\n"
        "        Scan ['test']\n"
        "          BindBlock:\n"
        "            [ptest]\n"
        "              Source []\n",
        ExplainGenerator::explain(rootNode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "  RefBlock: \n"
        "  CollationNode []\n"
        "    collation:\n"
        "      ptest: Ascending\n"
        "    RefBlock: \n"
        "      Variable [ptest]\n"
        "    Filter []\n"
        "      EvalFilter []\n"
        "        PathIdentity []\n"
        "        Variable [P1]\n"
        "      Evaluation []\n"
        "        BindBlock:\n"
        "          [P1]\n"
        "            EvalPath []\n"
        "              PathIdentity []\n"
        "              Variable [ptest]\n"
        "        Scan ['test']\n"
        "          BindBlock:\n"
        "            [ptest]\n"
        "              Source []\n",
        ExplainGenerator::explain(latest));
}

TEST(LogicalOptimizer, FilterProjectComplexRewrite) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projection2Node = make<EvaluationNode>(
        "p2", make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")), std::move(scanNode));

    ABT projection3Node =
        make<EvaluationNode>("p3",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(projection2Node));

    ABT collationNode = make<CollationNode>(
        CollationRequirement({{"ptest", CollationOp::Ascending}}), std::move(projection3Node));

    ABT projection1Node =
        make<EvaluationNode>("p1",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(collationNode));

    ABT filter1Node = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("p1")),
                                       std::move(projection1Node));

    ABT filterScanNode = make<FilterNode>(
        make<EvalFilter>(make<PathIdentity>(), make<Variable>("ptest")), std::move(filter1Node));

    ABT filter2Node = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("p2")),
                                       std::move(filterScanNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{{}}, std::move(filter2Node));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   RefBlock: \n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p2]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [ptest]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p1]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p1]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       ptest: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [ptest]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p3]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p2]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(rootNode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

    // Note: this assert depends on the order on which we consider rewrites.
    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   RefBlock: \n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p1]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p1]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [ptest]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       ptest: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [ptest]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p3]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Filter []\n"
        "|   EvalFilter []\n"
        "|   |   PathIdentity []\n"
        "|   Variable [p2]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [p2]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(latest));
}

TEST(LogicalOptimizer, FilterProjectGroupRewrite) {
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

    ABT filterANode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("a")),
                                       std::move(groupByNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"c"}},
                                  std::move(filterANode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       c\n"
        "|   RefBlock: \n"
        "|       Variable [c]\n"
        "GroupBy []\n"
        "|   |   groupings:\n"
        "|   |       RefBlock: \n"
        "|   |           Variable [a]\n"
        "|   aggregations:\n"
        "|       [c]\n"
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
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(latest));
}

TEST(LogicalOptimizer, FilterProjectUnwindRewrite) {
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

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

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
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(latest));
}

TEST(LogicalOptimizer, FilterProjectExchangeRewrite) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projectionANode = make<EvaluationNode>(
        "a", make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")), std::move(scanNode));
    ABT projectionBNode =
        make<EvaluationNode>("b",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(projectionANode));

    ABT exchangeNode = make<ExchangeNode>(
        properties::DistributionRequirement(DistributionType::HashPartitioning, {"a"}),
        false /*preserveSort*/,
        std::move(projectionBNode));

    ABT filterANode = make<FilterNode>(make<EvalFilter>(make<PathIdentity>(), make<Variable>("a")),
                                       std::move(exchangeNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"a", "b"}},
                                  std::move(filterANode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       a\n"
        "|   |       b\n"
        "|   RefBlock: \n"
        "|       Variable [a]\n"
        "|       Variable [b]\n"
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
        "Exchange []\n"
        "|   |   distribution:\n"
        "|   |       type: HashPartitioning\n"
        "|   |       projections:\n"
        "|   |           a\n"
        "|   RefBlock: \n"
        "|       Variable [a]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(latest));
}

TEST(LogicalOptimizer, UnwindCollationRewrite) {
    using namespace properties;
    PrefixId prefixId;

    ABT scanNode = make<ScanNode>("ptest", "test");

    ABT projectionANode = make<EvaluationNode>(
        "a", make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")), std::move(scanNode));
    ABT projectionBNode =
        make<EvaluationNode>("b",
                             make<EvalPath>(make<PathIdentity>(), make<Variable>("ptest")),
                             std::move(projectionANode));

    // This collation node should stay below the unwind.
    ABT collationANode = make<CollationNode>(CollationRequirement({{"a", CollationOp::Ascending}}),
                                             std::move(projectionBNode));

    // This collation node should go above the unwind.
    ABT collationBNode = make<CollationNode>(CollationRequirement({{"b", CollationOp::Ascending}}),
                                             std::move(collationANode));

    ABT unwindNode =
        make<UnwindNode>("a", "a_pid", false /*retainNonArrays*/, std::move(collationBNode));

    ABT rootNode = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"a", "b"}},
                                  std::move(unwindNode));

    OptPhaseManager phaseManager({OptPhaseManager::OptPhase::MemoLogicalRewritePhase},
                                 prefixId,
                                 {{{"test", {{}, {}}}}},
                                 DebugInfo::kDefaultForTests);
    ABT latest = std::move(rootNode);
    ASSERT_TRUE(phaseManager.optimize(latest));

    ASSERT_EQ(
        "RootNode []\n"
        "|   |   projections:\n"
        "|   |       a\n"
        "|   |       b\n"
        "|   RefBlock: \n"
        "|       Variable [a]\n"
        "|       Variable [b]\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       b: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [b]\n"
        "Unwind []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           Source []\n"
        "|       [a_pid]\n"
        "|           Source []\n"
        "CollationNode []\n"
        "|   |   collation:\n"
        "|   |       a: Ascending\n"
        "|   RefBlock: \n"
        "|       Variable [a]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [a]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Evaluation []\n"
        "|   BindBlock:\n"
        "|       [b]\n"
        "|           EvalPath []\n"
        "|           |   PathIdentity []\n"
        "|           Variable [ptest]\n"
        "Scan ['test']\n"
        "    BindBlock:\n"
        "        [ptest]\n"
        "            Source []\n",
        ExplainGenerator::explainV2(latest));
}

}  // namespace
}  // namespace mongo::optimizer
