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

#pragma once

#include "mongo/db/query/optimizer/reference_tracker.h"

namespace mongo::optimizer {
/**
 * This is an example rewriter that does constant evaluation in-place.
 */
class ConstEval {
public:
    ConstEval(VariableEnvironment& env) : _env(env) {}

    // The default noop transport. Note the first ABT& parameter.
    template <typename T, typename... Ts>
    void transport(ABT&, const T&, Ts&&...) {}

    void transport(ABT& n, const Variable& var);

    void prepare(ABT&, const Let& let);
    void transport(ABT& n, const Let& let, ABT&, ABT& in);
    void transport(ABT& n, const LambdaApplication& app, ABT& lam, ABT& arg);
    void prepare(ABT&, const LambdaAbstraction&);
    void transport(ABT&, const LambdaAbstraction&, ABT&);

    // Specific transport for binary operation
    // The const correctness is probably wrong (as const ABT& lhs, const ABT& rhs does not work for
    // some reason but we can fix it later).
    void transport(ABT& n, const BinaryOp& op, ABT& lhs, ABT& rhs);
    void transport(ABT& n, const FunctionCall& op, std::vector<ABT>& args);
    void transport(ABT& n, const If& op, ABT& cond, ABT& thenBranch, ABT& elseBranch);
    void transport(ABT& n, const EvaluationNode& op, ABT& child, ABT& expr);

    void prepare(ABT&, const PathTraverse&);
    void transport(ABT&, const PathTraverse&, ABT&);

    void prepare(ABT&, const References& refs);
    void transport(ABT& n, const References& op, std::vector<ABT>&);

    // The tree is passed in as NON-const reference as we will be updating it.
    bool optimize(ABT& n);

private:
    void swapAndUpdate(ABT& n, ABT newN);
    void removeUnusedEvalNodes();

    VariableEnvironment& _env;
    stdx::unordered_set<const Variable*> _singleRef;
    stdx::unordered_set<const EvaluationNode*> _noRefProj;
    stdx::unordered_map<const Let*, std::vector<const Variable*>> _letRefs;
    stdx::unordered_map<const EvaluationNode*, std::vector<const Variable*>> _projectRefs;

    struct RefHash {
        size_t operator()(const ABT::reference_type& nodeRef) const {
            return nodeRef.hash();
        }
    };

    stdx::unordered_map<ABT::reference_type, ABT::reference_type, RefHash> _staleDefs;
    // We collect old ABTs in order to avoid the ABA problem.
    std::vector<ABT> _staleABTs;

    bool _inRefBlock{false};
    size_t _inCostlyCtx{0};
    bool _changed{false};
};

}  // namespace mongo::optimizer
