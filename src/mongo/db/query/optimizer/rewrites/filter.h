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

#pragma once

#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer {

/**
 * This rewriter currently performs two functions:
 *  1. If the filter node's condition involves a top-level multiplicative composition, two new
 * filter nodes are created for each side of the composition.
 *  2. If the filter node has a top-level PathGet, an evaluation node is extracted for the path get,
 * and a new filter node is created with inner path.
 */
class FilterRewriter {
public:
    FilterRewriter(PrefixId& prefixId) : _prefixId(prefixId) {}

    // The default noop transport.
    template <typename T, typename... Ts>
    void transport(ABT&, const T&, Ts&&...) {}

    void transport(ABT& n, const FilterNode& /*node*/, ABT& child, ABT& filter);

    bool optimize(ABT& n);

private:
    PrefixId& _prefixId;
    bool _changed{false};
};

}  // namespace mongo::optimizer
