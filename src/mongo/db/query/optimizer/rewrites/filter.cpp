/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#include "mongo/db/query/optimizer/rewrites/filter.h"

namespace mongo::optimizer {

void FilterRewriter::transport(ABT& n, const FilterNode& /*node*/, ABT& child, ABT& filter) {
    if (auto evalFilter = filter.cast<EvalFilter>(); evalFilter != nullptr) {
        if (auto composition = evalFilter->getPath().cast<PathComposeM>(); composition != nullptr) {
            // Remove the path composition and insert two filter nodes.
            ABT filterNode1 = make<FilterNode>(
                make<EvalFilter>(composition->getPath1(), evalFilter->getInput()), child);
            ABT filterNode2 =
                make<FilterNode>(make<EvalFilter>(composition->getPath2(), evalFilter->getInput()),
                                 std::move(filterNode1));

            std::swap(n, filterNode2);
            _changed = true;
        } else if (auto pathGet = evalFilter->getPath().cast<PathGet>();
                   pathGet != nullptr && pathGet->getPath().cast<PathIdentity>() == nullptr) {
            // Insert a new eval node to compute the path, and a new filter node over the
            // reduced filter path.

            ProjectionName tempProj = _prefixId.getNextId("evalTemp");
            ABT evalNode = make<EvaluationNode>(
                tempProj,
                make<EvalPath>(make<PathGet>(pathGet->name(), make<PathIdentity>()),
                               evalFilter->getInput()),
                child);

            ABT filterNode1 =
                make<FilterNode>(true /*isInputVarTemp*/,
                                 make<EvalFilter>(pathGet->getPath(), make<Variable>(tempProj)),
                                 std::move(evalNode));

            std::swap(n, filterNode1);
            _changed = true;
        }
    }
}

bool FilterRewriter::optimize(ABT& n) {
    do {
        _changed = false;
        algebra::transport<true>(n, *this);
    } while (_changed);

    return false;
}
}  // namespace mongo::optimizer
