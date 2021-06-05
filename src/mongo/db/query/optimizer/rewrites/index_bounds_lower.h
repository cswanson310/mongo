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
 * This rewriter currenly "lowers" any compound index bounds intervals generated after physical
 * optimization to "singleton" index intervals which sbe can execute. For example, an index node
 * with the interval [0, +inf) ^ (-inf, 10] is lowered to IndexScan[0, +inf] hashjoin
 * IndexScan[-inf, 10). Unioning of intervals is similarly lowered using a union node with
 * de-duplication.
 */
class IndexBoundsLowerRewriter {
public:
    IndexBoundsLowerRewriter(PrefixId& prefixId) : _prefixId(prefixId) {}

    // The default noop transport.
    template <typename T, typename... Ts>
    void transport(ABT&, const T&, Ts&&...){};

    void transport(ABT& n, const IndexScanNode& node, ABT& /*binder*/);

    bool optimize(ABT& n);

private:
    ABT createIntersectOrUnionForIndexLower(const bool isIntersect,
                                            ABTVector inputs,
                                            const FieldProjectionMap& innerMap,
                                            const FieldProjectionMap& outerMap);

    PrefixId& _prefixId;
};

}  // namespace mongo::optimizer
