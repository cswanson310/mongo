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

#include <queue>

#include "mongo/db/query/optimizer/cascades/memo.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

#define LOGICALREWRITER_NAMES(F)                                  \
    /* "Linear" reordering rewrites. */                           \
    F(FilterFilterReorder)                                        \
    F(EvaluationEvaluationReorder)                                \
    F(FilterEvaluationReorder)                                    \
    F(EvaluationFilterReorder)                                    \
    F(FilterCollationReorder)                                     \
    F(EvaluationCollationReorder)                                 \
    F(FilterLimitSkipReorder)                                     \
    F(EvaluationLimitSkipReorder)                                 \
                                                                  \
    F(FilterGroupByReorder)                                       \
    F(GroupCollationReorder)                                      \
                                                                  \
    F(FilterUnwindReorder)                                        \
    F(EvaluationUnwindReorder)                                    \
    F(UnwindCollationReorder)                                     \
    F(UnwindLimitSkipReorder)                                     \
                                                                  \
    F(FilterExchangeReorder)                                      \
    F(ExchangeEvaluationReorder)                                  \
    F(ExchangeCollationReorder)                                   \
    F(ExchangeLimitSkipReorder)                                   \
                                                                  \
    /* Merging rewrites. */                                       \
    F(CollationMerge)                                             \
    F(LimitSkipMerge)                                             \
    F(SargableMerge)                                              \
                                                                  \
    /* Local-global optimization for GroupBy */                   \
    F(GroupByLocalGlobal)                                         \
                                                                  \
    /* Convert filter and evaluation nodes into sargable nodes */ \
    F(SargableFilterConvert)                                      \
    F(SargableEvaluationConvert)                                  \
    F(SargableSplit)

MAKE_PRINTABLE_ENUM(LogicalRewriteType, LOGICALREWRITER_NAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(LogicalRewriterTypeEnum,
                                 LogicalRewriterType,
                                 LOGICALREWRITER_NAMES);
#undef LOGICALREWRITER_NAMES

class LogicalRewriter {
    friend struct RewriteContext;

public:
    using PriorityType = size_t;
    struct RewriteEntry {
        bool operator<(const RewriteEntry& other) const;

        LogicalRewriteType _type;
        PriorityType _priority;
        Memo::NodeId _nodeId;
    };

    using QueueType = std::priority_queue<RewriteEntry>;

    /**
     * Map of rewrite type to rewrite priority
     */
    using RewriteSet = std::unordered_map<LogicalRewriteType, PriorityType>;


    LogicalRewriter(Memo& memo,
                    PrefixId& prefixId,
                    const Metadata& metadata,
                    RewriteSet rewriteSet);

    GroupIdType addInitialNode(const ABT& node);
    GroupIdType addNode(const ABT& node, const GroupIdType targetGroupId);

    /**
     * Performs logical rewrites until a fix point is reached.
     * If debug limit is specified and the number of rewrite steps exceed it, an error (false) is
     * returned.
     */
    bool rewriteToFixPoint();

    /**
     * Returns plan defined by latest nodes inserted in the memo. Used for debugging.
     */
    ABT getLatestPlan(GroupIdType rootGroupId);

    static const RewriteSet& getRewriteSet();

    QueueType& getQueue();

private:
    using RewriteFn = std::function<void(LogicalRewriter* rewriter, const Memo::NodeId nodeId)>;
    using RewriteFnMap = std::unordered_map<LogicalRewriteType, RewriteFn>;

    /**
     * Attempts to perform a reordering rewrite specified by the R template argument.
     */
    template <class AboveType, class BelowType, template <class, class> class R>
    void bindAboveBelow(const Memo::NodeId nodeMemoId);

    /**
     * Attempts to perform a simple rewrite specified by the R template argument.
     */
    template <class Type, template <class> class R>
    void bindSingleNode(const Memo::NodeId nodeMemoId);

    RewriteFnMap initializeRewrites();

    static RewriteSet _rewriteSet;

    const RewriteSet _activeRewriteSet;

    QueueType _queue;

    // We don't own those:
    Memo& _memo;
    PrefixId& _prefixId;
    const Metadata& _metadata;
};


}  // namespace mongo::optimizer::cascades
