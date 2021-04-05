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

#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/rewrites/const_eval.h"
#include "mongo/db/query/optimizer/rewrites/path.h"
#include "mongo/db/query/optimizer/rewrites/path_lower.h"
#include "mongo/unittest/unittest.h"

namespace mongo::optimizer {
namespace {
TEST(Path, Const) {
    auto tree = make<EvalPath>(make<PathConstant>(Constant::int64(2)), make<Variable>("ptest"));
    auto env = VariableEnvironment::build(tree);

    auto fusor = PathFusion(env);
    fusor.optimize(tree);

    // The result must be Constant.
    auto result = tree.cast<Constant>();
    ASSERT(result != nullptr);

    // And the value must be 2
    ASSERT_EQ(result->getValueInt64(), 2);
}

TEST(Path, GetConst) {
    // Get "any" Const 2
    auto tree = make<EvalPath>(make<PathGet>("any", make<PathConstant>(Constant::int64(2))),
                               make<Variable>("ptest"));
    auto env = VariableEnvironment::build(tree);

    auto fusor = PathFusion(env);
    fusor.optimize(tree);

    // The result must be Constant.
    auto result = tree.cast<Constant>();
    ASSERT(result != nullptr);

    // And the value must be 2
    ASSERT_EQ(result->getValueInt64(), 2);
}

TEST(Path, Fuse1) {
    // Field "a" Const 2
    auto field = make<EvalPath>(make<PathField>("a", make<PathConstant>(Constant::int64(2))),
                                make<Variable>("root"));

    // Get "a" Id
    auto get = make<EvalPath>(make<PathGet>("a", make<PathIdentity>()), make<Variable>("x"));

    // let x = (Field "a" Const 2 | root)
    //     in  (Get "a" Id | x)
    auto tree = make<Let>("x", std::move(field), std::move(get));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathFusion{env}.optimize(tree)) {
            changed = true;
        }

        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }

    } while (changed);

    // The result must be Constant.
    auto result = tree.cast<Constant>();
    ASSERT(result != nullptr);

    // And the value must be 2
    ASSERT_EQ(result->getValueInt64(), 2);
}
TEST(Path, Fuse2) {
    auto scanNode = make<ScanNode>("root", "test");

    // Field "a" Const 2
    auto field = make<EvalPath>(make<PathField>("a", make<PathConstant>(Constant::int64(2))),
                                make<Variable>("root"));

    auto project1 = make<EvaluationNode>("x", std::move(field), std::move(scanNode));

    // Get "a" Id
    auto get = make<EvalPath>(make<PathGet>("a", make<PathIdentity>()), make<Variable>("x"));
    auto project2 = make<EvaluationNode>("y", std::move(get), std::move(project1));

    auto tree = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"y"}},
                               std::move(project2));

    auto env = VariableEnvironment::build(tree);
    {
        ProjectionNameSet projSet = env.topLevelProjections();
        ProjectionNameSet expSet = {"x", "y", "root"};
        ASSERT(expSet == projSet);
        ASSERT(!env.hasFreeVariables());
    }

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathFusion{env}.optimize(tree)) {
            changed = true;
        }

        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }

    } while (changed);

    // After rewrites for x projection disappear from the tree.
    {
        ProjectionNameSet projSet = env.topLevelProjections();
        ProjectionNameSet expSet = {"root", "y"};
        ASSERT(expSet == projSet);
        ASSERT(!env.hasFreeVariables());
    }
}

TEST(Path, Lower1) {
    auto tree = make<EvalPath>(make<PathIdentity>(), make<Variable>("foo"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Variable>());
    ASSERT_EQ(tree.cast<Variable>()->name(), "foo");
}

TEST(Path, Lower2) {
    auto tree = make<EvalPath>(make<PathConstant>(Constant::int64(10)), make<Variable>("foo"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Constant>());
    ASSERT_EQ(tree.cast<Constant>()->getValueInt64(), 10);
}

TEST(Path, Lower3) {
    auto tree = make<EvalPath>(
        make<PathLambda>(make<LambdaAbstraction>(
            "x", make<BinaryOp>(Operations::Add, make<Variable>("x"), Constant::int64(1)))),
        Constant::int64(9));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Constant>());
    ASSERT_EQ(tree.cast<Constant>()->getValueInt64(), 10);
}

TEST(Path, Lower4) {
    auto tree = make<EvalPath>(
        make<PathGet>(
            "fieldA",
            make<PathGet>("fieldB",
                          /*make<PathIdentity>()*/ make<PathConstant>(Constant::int64(100)))),
        make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Constant>());
    ASSERT_EQ(tree.cast<Constant>()->getValueInt64(), 100);
}

TEST(Path, Lower5) {
    auto tree = make<EvalPath>(
        make<PathGet>(
            "fieldA",
            make<PathGet>(
                "fieldB",
                make<PathLambda>(make<LambdaAbstraction>(
                    "x",
                    make<BinaryOp>(Operations::Add, make<Variable>("x"), Constant::int64(1)))))),
        make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT_EQ(
        "BinaryOp [Add]\n"
        "  FunctionCall [getField]\n"
        "    FunctionCall [getField]\n"
        "      Variable [rootObj]\n"
        "      Const [\"fieldA\"]\n"
        "    Const [\"fieldB\"]\n"
        "  Const [1]\n",
        ExplainGenerator::explain(tree));
}

TEST(Path, ProjElim1) {
    auto scanNode = make<ScanNode>("root", "test");

    auto expr1 = make<FunctionCall>("anyFunctionWillDo", makeSeq(make<Variable>("root")));
    auto project1 = make<EvaluationNode>("x", std::move(expr1), std::move(scanNode));

    auto expr2 = make<Variable>("x");
    auto project2 = make<EvaluationNode>("y", std::move(expr2), std::move(project1));

    auto tree = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{"y"}},
                               std::move(project2));

    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "    y\n"
        "  RefBlock: \n"
        "    Variable [y]\n"
        "  Evaluation []\n"
        "    BindBlock:\n"
        "      [y]\n"
        "        FunctionCall [anyFunctionWillDo]\n"
        "          Variable [root]\n"
        "    Scan ['test']\n"
        "      BindBlock:\n"
        "        [root]\n"
        "          Source []\n",
        ExplainGenerator::explain(tree));
}

TEST(Path, ProjElim2) {
    auto scanNode = make<ScanNode>("root", "test");

    auto expr1 = make<FunctionCall>("anyFunctionWillDo", makeSeq(make<Variable>("root")));
    auto project1 = make<EvaluationNode>("x", std::move(expr1), std::move(scanNode));

    auto expr2 = make<Variable>("x");
    auto project2 = make<EvaluationNode>("y", std::move(expr2), std::move(project1));

    auto tree = make<RootNode>(properties::ProjectionRequirement{{}}, std::move(project2));

    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "  RefBlock: \n"
        "  Scan ['test']\n"
        "    BindBlock:\n"
        "      [root]\n"
        "        Source []\n",
        ExplainGenerator::explain(tree));
}

TEST(Path, ProjElim3) {
    auto node = make<ScanNode>("root", "test");
    std::string var = "root";
    for (int i = 0; i < 100; ++i) {
        std::string newVar = "p" + std::to_string(i);
        node = make<EvaluationNode>(
            newVar,
            // make<FunctionCall>("anyFunctionWillDo", makeSeq(make<Variable>(var))),
            make<Variable>(var),
            std::move(node));
        var = newVar;
    }

    auto tree = make<RootNode>(properties::ProjectionRequirement{ProjectionNameVector{var}},
                               std::move(node));

    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT_EQ(
        "RootNode []\n"
        "  projections:\n"
        "    p99\n"
        "  RefBlock: \n"
        "    Variable [p99]\n"
        "  Evaluation []\n"
        "    BindBlock:\n"
        "      [p99]\n"
        "        Variable [root]\n"
        "    Scan ['test']\n"
        "      BindBlock:\n"
        "        [root]\n"
        "          Source []\n",
        ExplainGenerator::explain(tree));
}

TEST(Path, Lower6) {
    auto tree = make<EvalPath>(
        make<PathGet>("fieldA", make<PathGet>("fieldB", make<PathDefault>(Constant::int64(0)))),
        make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT_EQ(
        "Let [valDefault8]\n"
        "  FunctionCall [getField]\n"
        "    FunctionCall [getField]\n"
        "      Variable [rootObj]\n"
        "      Const [\"fieldA\"]\n"
        "    Const [\"fieldB\"]\n"
        "  If []\n"
        "    FunctionCall [exists]\n"
        "      Variable [valDefault8]\n"
        "    Variable [valDefault8]\n"
        "    Const [0]\n",
        ExplainGenerator::explain(tree));
}

TEST(Path, Lower7) {
    auto tree = make<EvalPath>(make<PathGet>("fieldA",
                                             make<PathTraverse>(make<PathGet>(
                                                 "fieldB", make<PathDefault>(Constant::int64(0))))),
                               make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    // Add some asserts on the shape of the tree or something.
}

TEST(Path, Lower8) {
    auto tree = make<EvalPath>(
        make<PathComposeM>(make<PathIdentity>(), make<PathConstant>(Constant::int64(100))),
        make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Constant>());
    ASSERT_EQ(tree.cast<Constant>()->getValueInt64(), 100);
}

TEST(Path, Lower9) {
    auto tree = make<EvalPath>(make<PathComposeM>(make<PathGet>("fieldA", make<PathIdentity>()),
                                                  make<PathConstant>(Constant::int64(100))),
                               make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    ASSERT(tree.is<Constant>());
    ASSERT_EQ(tree.cast<Constant>()->getValueInt64(), 100);
}

TEST(Path, Lower10) {
    auto tree = make<EvalPath>(
        make<PathField>(
            "fieldA",
            make<PathTraverse>(make<PathField>("fieldB", make<PathDefault>(Constant::int64(0))))),
        make<Variable>("rootObj"));
    auto env = VariableEnvironment::build(tree);

    // Run rewriters while things change
    bool changed = false;
    do {
        changed = false;
        if (PathLowering{env}.optimize(tree)) {
            changed = true;
        }
        if (ConstEval{env}.optimize(tree)) {
            changed = true;
        }
    } while (changed);

    // Add some asserts on the shape of the tree or something.
}

}  // namespace
}  // namespace mongo::optimizer
