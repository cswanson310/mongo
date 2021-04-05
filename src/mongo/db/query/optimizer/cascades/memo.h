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

#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/reference_tracker.h"

namespace mongo::optimizer::cascades {

class OrderPreservingABTSet {
public:
    OrderPreservingABTSet() = default;
    OrderPreservingABTSet(const OrderPreservingABTSet&) = delete;
    OrderPreservingABTSet(OrderPreservingABTSet&&) = default;

    ABT::reference_type at(size_t index) const;
    std::pair<size_t, bool> emplace_back(ABT node);
    std::pair<size_t, bool> find(ABT::reference_type node) const;

    size_t size() const;
    const ABTVector& getVector() const;

private:
    struct MemoNodeRefHash {
        size_t operator()(const ABT::reference_type& nodeRef) const;
    };

    struct MemoNodeRefCompare {
        bool operator()(const ABT::reference_type& left, const ABT::reference_type& right) const;
    };

    std::unordered_map<ABT::reference_type, size_t, MemoNodeRefHash, MemoNodeRefCompare> _map;
    ABTVector _vector;
};

using ChildPropsType = std::vector<std::pair<ABT*, properties::Properties>>;

/**
 * Keeps track of candidate physical rewrites.
 */
struct PhysRewriteEntry {
    PhysRewriteEntry() = delete;
    PhysRewriteEntry(ABT node, ChildPropsType childProps)
        : _node(std::move(node)), _childProps(std::move(childProps)) {}

    PhysRewriteEntry(const PhysRewriteEntry& other) = delete;
    PhysRewriteEntry(PhysRewriteEntry&& other) = default;

    ABT _node;
    ChildPropsType _childProps;
};

using PhysRewriteQueue = std::queue<std::unique_ptr<PhysRewriteEntry>>;

struct PhysOptimizationResult {
    PhysOptimizationResult();
    PhysOptimizationResult(size_t index, properties::Properties physProps, CostType costLimit);

    bool hasResult() const;
    bool isOptimized() const;

    const size_t _index;
    const properties::Properties _physProps;
    const CostType _costLimit;

    ABT _node;
    CostType _cost;

    PhysRewriteQueue _queue;
};

struct Group {
    using PhysicalNodeResults = std::vector<std::unique_ptr<PhysOptimizationResult>>;

    explicit Group(ProjectionNameSet projections);

    Group(const Group&) = delete;
    Group(Group&&) = default;

    const ExpressionBinder& binder() const;

    PhysOptimizationResult& addOptimizationResult(properties::Properties properties,
                                                  CostType costLimit);

    // Associated logical nodes.
    OrderPreservingABTSet _logicalNodes;
    // Group logical properties.
    properties::Properties _logicalProperties;
    ABT _binder;

    // Best physical plan for given physical properties: aka "Winner's circle".
    PhysicalNodeResults _physicalNodes;
};

using LogicalPropsDeriveFn =
    std::function<properties::Properties(const Memo& memo, const GroupIdType groupId)>;

using CEDeriveFn = std::function<CEType(const Memo& memo, const GroupIdType groupId)>;

class Memo {
public:
    using GroupIdVector = std::vector<GroupIdType>;
    using NodeId = std::pair<GroupIdType, size_t>;
    using NodeIdSet = std::set<NodeId>;

    using InputGroupsToNodeIdMap = std::map<GroupIdVector, NodeIdSet>;

    struct NodeTargetGroupHash {
        size_t operator()(const ABT::reference_type& nodeRef) const {
            return std::hash<const Node*>()(nodeRef.cast<Node>());
        }
    };
    using NodeTargetGroupMap =
        std::unordered_map<ABT::reference_type, GroupIdType, NodeTargetGroupHash>;

    Memo(DebugInfo debugInfo, LogicalPropsDeriveFn logicalPropsDeriveFn, CEDeriveFn ceDeriveFn);

    const Group& getGroup(GroupIdType groupId) const;
    Group& getGroup(GroupIdType groupId);

    std::pair<size_t, bool> findNodeInGroup(GroupIdType groupId, ABT::reference_type node) const;

    size_t getGroupCount() const;

    ABT::reference_type getNode(NodeId nodeMemoId) const;

    NodeId addNode(GroupIdVector groupVector,
                   ProjectionNameSet projections,
                   const GroupIdType targetGroupId,
                   NodeIdSet& insertedNodeIds,
                   ABT n);

    GroupIdType integrate(const ABT& node,
                          NodeTargetGroupMap targetGroupMap,
                          NodeIdSet& insertedNodeIds);

    const InputGroupsToNodeIdMap& getInputGroupsToNodeIdMap() const;

    const DebugInfo& getDebugInfo() const;

    void clear();

private:
    GroupIdType addGroup(ProjectionNameSet projections);

    std::pair<NodeId, bool> addNode(GroupIdType groupId, ABT n);

    std::pair<NodeId, bool> findNode(const GroupIdVector& groups, const ABT& node);

    std::vector<Group> _groups;

    // Used to find nodes using particular groups as inputs.
    InputGroupsToNodeIdMap _inputGroupsToNodeIdMap;

    const LogicalPropsDeriveFn _logicalPropsDeriveFn;
    const CEDeriveFn _ceDeriveFn;

    const DebugInfo _debugInfo;
};

}  // namespace mongo::optimizer::cascades
