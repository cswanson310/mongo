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

#include <set>

#include "mongo/db/query/optimizer/abt_compare.h"
#include "mongo/db/query/optimizer/abt_hash.h"
#include "mongo/db/query/optimizer/cascades/memo.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer::cascades {

size_t OrderPreservingABTSet::MemoNodeRefHash::operator()(
    const ABT::reference_type& nodeRef) const {
    return ABTHashGenerator::generateNoChildrenRef(nodeRef);
}

bool OrderPreservingABTSet::MemoNodeRefCompare::operator()(const ABT::reference_type& left,
                                                           const ABT::reference_type& right) const {
    return ABTCompare::compareNoChildrenRef(left, right);
}

PhysOptimizationResult::PhysOptimizationResult()
    : PhysOptimizationResult(0, {}, CostType::kInfinity) {}

PhysOptimizationResult::PhysOptimizationResult(size_t index,
                                               properties::Properties physProps,
                                               CostType costLimit)
    : _index(index),
      _physProps(std::move(physProps)),
      _costLimit(std::move(costLimit)),
      _node(make<Blackhole>()),
      _cost(CostType::kInfinity),
      _queue() {}

bool PhysOptimizationResult::hasResult() const {
    return _cost != CostType::kInfinity && _node != make<Blackhole>();
}

bool PhysOptimizationResult::isOptimized() const {
    return _queue.empty();
}

ABT::reference_type OrderPreservingABTSet::at(const size_t index) const {
    return _vector.at(index).ref();
}

std::pair<size_t, bool> OrderPreservingABTSet::emplace_back(ABT node) {
    auto [index, found] = find(node.ref());
    if (found) {
        return {index, false};
    }

    const size_t id = _vector.size();
    _vector.emplace_back(std::move(node));
    _map.emplace(_vector.back().ref(), id);
    return {id, true};
}

std::pair<size_t, bool> OrderPreservingABTSet::find(ABT::reference_type node) const {
    auto it = _map.find(node);
    if (it == _map.end()) {
        return {0, false};
    }

    return {it->second, true};
}

size_t OrderPreservingABTSet::size() const {
    return _vector.size();
}

const ABTVector& OrderPreservingABTSet::getVector() const {
    return _vector;
}

static ABT createBinderMap(const properties::Properties& logicalProperties) {
    const properties::ProjectionAvailability& projSet =
        properties::getPropertyConst<properties::ProjectionAvailability>(logicalProperties);

    ProjectionNameVector projectionVector;
    ABTVector expressions;

    ProjectionNameOrderedSet ordered = convertToOrderedSet(projSet.getProjections());
    for (const ProjectionName& projection : ordered) {
        projectionVector.push_back(projection);
        expressions.emplace_back(make<Source>());
    }

    return make<ExpressionBinder>(std::move(projectionVector), std::move(expressions));
}

Group::Group(ProjectionNameSet projections)
    : _logicalNodes(),
      _logicalProperties(
          properties::makeProperties(properties::ProjectionAvailability(std::move(projections)))),
      _binder(createBinderMap(_logicalProperties)) {}

const ExpressionBinder& Group::binder() const {
    auto pointer = _binder.cast<ExpressionBinder>();
    uassert(6624000, "Invalid binder type", pointer);

    return *pointer;
}

PhysOptimizationResult& Group::addOptimizationResult(properties::Properties properties,
                                                     CostType costLimit) {
    return *_physicalNodes.emplace_back(std::make_unique<PhysOptimizationResult>(
        _physicalNodes.size(), std::move(properties), std::move(costLimit)));
}

class MemoIntegrator {
public:
    explicit MemoIntegrator(Memo& memo,
                            Memo::NodeTargetGroupMap targetGroupMap,
                            Memo::NodeIdSet& insertedNodeIds)
        : _memo(memo),
          _insertedNodeIds(insertedNodeIds),
          _targetGroupMap(std::move(targetGroupMap)) {}

    /**
     * Nodes
     */
    void prepare(const ABT& n, const ScanNode& node, const VariableEnvironment& /*env*/) {
        // noop
    }

    GroupIdType transport(const ABT& n,
                          const ScanNode& node,
                          const VariableEnvironment& env,
                          GroupIdType /*binder*/) {
        return addNodes(n, node, n, env, {});
    }

    void prepare(const ABT& n,
                 const MemoLogicalDelegatorNode& node,
                 const VariableEnvironment& /*env*/) {
        // noop
    }

    GroupIdType transport(const ABT& /*n*/,
                          const MemoLogicalDelegatorNode& node,
                          const VariableEnvironment& /*env*/) {
        return node.getGroupId();
    }

    void prepare(const ABT& n, const FilterNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const FilterNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binder*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const EvaluationNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const EvaluationNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binder*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const SargableNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const SargableNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binder*/,
                          GroupIdType /*references*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const RIDIntersectNode& node, const VariableEnvironment& /*env*/) {
        // noop.
    }

    GroupIdType transport(const ABT& n,
                          const RIDIntersectNode& node,
                          const VariableEnvironment& env,
                          GroupIdType leftChild,
                          GroupIdType rightChild) {
        return addNodes(n, node, env, leftChild, rightChild);
    }

    void prepare(const ABT& n, const BinaryJoinNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapBinary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const BinaryJoinNode& node,
                          const VariableEnvironment& env,
                          GroupIdType leftChild,
                          GroupIdType rightChild,
                          GroupIdType /*filter*/) {
        return addNodes(n, node, env, leftChild, rightChild);
    }

    void prepare(const ABT& n, const InnerMultiJoinNode& node, const VariableEnvironment& /*env*/) {
        // noop
    }

    GroupIdType transport(const ABT& n,
                          const InnerMultiJoinNode& node,
                          const VariableEnvironment& env,
                          Memo::GroupIdVector children) {
        return addNodes(n, node, env, std::move(children));
    }

    void prepare(const ABT& n, const UnionNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapNary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const UnionNode& node,
                          const VariableEnvironment& env,
                          Memo::GroupIdVector children,
                          GroupIdType /*binder*/,
                          GroupIdType /*refs*/) {
        return addNodes(n, node, env, std::move(children));
    }

    void prepare(const ABT& n, const GroupByNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const GroupByNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binderAgg*/,
                          GroupIdType /*refsAgg*/,
                          GroupIdType /*binderGb*/,
                          GroupIdType /*refsGb*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const UnwindNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const UnwindNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binder*/,
                          GroupIdType /*refs*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const WindNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const WindNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*binder*/,
                          GroupIdType /*refs*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const CollationNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const CollationNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*refs*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const LimitSkipNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const LimitSkipNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const ExchangeNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const ExchangeNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*refs*/) {
        return addNode(n, node, env, child);
    }

    void prepare(const ABT& n, const RootNode& node, const VariableEnvironment& /*env*/) {
        updateTargetGroupMapUnary(n, node);
    }

    GroupIdType transport(const ABT& n,
                          const RootNode& node,
                          const VariableEnvironment& env,
                          GroupIdType child,
                          GroupIdType /*refs*/) {
        return addNode(n, node, env, child);
    }

    /**
     * Other ABT types.
     */
    template <typename T, typename... Ts>
    GroupIdType transport(const ABT& /*n*/,
                          const T& /*node*/,
                          const VariableEnvironment& /*env*/,
                          Ts&&...) {
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its transport.");
        return -1;
    }

    template <typename T, typename... Ts>
    void prepare(const ABT& n, const T& /*node*/, const VariableEnvironment& /*env*/) {
        static_assert(!canBeLogicalNode<T>(), "Logical node must implement its prepare.");
    }

    GroupIdType integrate(const ABT& n) {
        return algebra::transport<true>(n, *this, VariableEnvironment::build(n, &_memo));
    }

private:
    GroupIdType addNodes(const ABT& n,
                         const Node& node,
                         ABT forMemo,
                         const VariableEnvironment& env,
                         Memo::GroupIdVector childGroupIds) {
        auto it = _targetGroupMap.find(n.ref());
        const GroupIdType targetGroupId = (it == _targetGroupMap.cend()) ? -1 : it->second;
        const auto result = _memo.addNode(std::move(childGroupIds),
                                          env.getProjections(&node),
                                          targetGroupId,
                                          _insertedNodeIds,
                                          std::move(forMemo));
        return result.first;
    }

    template <class T, typename... Args>
    GroupIdType addNodes(const ABT& n,
                         const T& node,
                         const VariableEnvironment& env,
                         Memo::GroupIdVector childGroupIds) {
        ABT forMemo = n;
        auto& childNodes = forMemo.template cast<T>()->nodes();
        for (size_t i = 0; i < childNodes.size(); i++) {
            const GroupIdType childGroupId = childGroupIds.at(i);
            uassert(0, "Invalid child group", childGroupId >= 0);
            childNodes.at(i) = make<MemoLogicalDelegatorNode>(childGroupId);
        }

        return addNodes(n, node, std::move(forMemo), env, std::move(childGroupIds));
    }

    template <class T>
    GroupIdType addNode(const ABT& n,
                        const T& node,
                        const VariableEnvironment& env,
                        GroupIdType childGroupId) {
        ABT forMemo = n;
        uassert(0, "Invalid child group", childGroupId >= 0);
        forMemo.cast<T>()->getChild() = make<MemoLogicalDelegatorNode>(childGroupId);
        return addNodes(n, node, std::move(forMemo), env, {childGroupId});
    }

    template <class T>
    GroupIdType addNodes(const ABT& n,
                         const T& node,
                         const VariableEnvironment& env,
                         GroupIdType leftGroupId,
                         GroupIdType rightGroupId) {
        ABT forMemo = n;
        uassert(0, "Invalid left child group", leftGroupId >= 0);
        uassert(0, "Invalid right child group", rightGroupId >= 0);

        forMemo.cast<T>()->getLeftChild() = make<MemoLogicalDelegatorNode>(leftGroupId);
        forMemo.cast<T>()->getRightChild() = make<MemoLogicalDelegatorNode>(rightGroupId);
        return addNodes(n, node, std::move(forMemo), env, {leftGroupId, rightGroupId});
    }

    template <class T>
    ABT::reference_type findExistingNodeFromTargetGroupMap(const ABT& n, const T& node) {
        auto it = _targetGroupMap.find(n.ref());
        if (it == _targetGroupMap.cend()) {
            return nullptr;
        }
        const auto [index, found] = _memo.findNodeInGroup(it->second, n.ref());
        if (!found) {
            return nullptr;
        }

        ABT::reference_type result = _memo.getNode({it->second, index});
        uassert(6624000, "Node type in memo does not match target type", result.is<T>());
        return result;
    }

    void updateTargetGroupRefs(
        const std::vector<std::pair<ABT::reference_type, GroupIdType>>& childGroups) {
        for (auto [childRef, targetGroupId] : childGroups) {
            auto it = _targetGroupMap.find(childRef);
            if (it == _targetGroupMap.cend()) {
                _targetGroupMap.emplace(childRef, targetGroupId);
            } else if (it->second != targetGroupId) {
                uasserted(6624000, "Incompatible target groups for parent and child");
            }
        }
    }

    template <class T>
    void updateTargetGroupMapUnary(const ABT& n, const T& node) {
        ABT::reference_type existing = findExistingNodeFromTargetGroupMap(n, node);
        if (!existing.empty()) {
            const GroupIdType targetGroupId = existing.cast<T>()
                                                  ->getChild()
                                                  .template cast<MemoLogicalDelegatorNode>()
                                                  ->getGroupId();
            updateTargetGroupRefs({{node.getChild().ref(), targetGroupId}});
        }
    }

    template <class T>
    void updateTargetGroupMapNary(const ABT& n, const T& node) {
        ABT::reference_type existing = findExistingNodeFromTargetGroupMap(n, node);
        if (!existing.empty()) {
            const ABTVector& existingChildren = existing.cast<T>()->nodes();
            const ABTVector& targetChildren = node.nodes();
            uassert(6624000,
                    "Different number of children between existing and target node",
                    existingChildren.size() == targetChildren.size());

            std::vector<std::pair<ABT::reference_type, GroupIdType>> childGroups;
            for (size_t i = 0; i < existingChildren.size(); i++) {
                const ABT& existingChild = existingChildren.at(i);
                const ABT& targetChild = targetChildren.at(i);
                childGroups.emplace_back(
                    targetChild.ref(),
                    existingChild.cast<MemoLogicalDelegatorNode>()->getGroupId());
            }
            updateTargetGroupRefs(childGroups);
        }
    }

    template <class T>
    void updateTargetGroupMapBinary(const ABT& n, const T& node) {
        ABT::reference_type existing = findExistingNodeFromTargetGroupMap(n, node);
        if (existing.empty()) {
            return;
        }

        const T& existingNode = *existing.cast<T>();
        const GroupIdType leftGroupId =
            existingNode.getLeftChild().template cast<MemoLogicalDelegatorNode>()->getGroupId();
        const GroupIdType rightGroupId =
            existingNode.getRightChild().template cast<MemoLogicalDelegatorNode>()->getGroupId();
        updateTargetGroupRefs(
            {{node.getLeftChild().ref(), leftGroupId}, {node.getRightChild().ref(), rightGroupId}});
    }

    /**
     * We do not own any of these.
     */
    Memo& _memo;
    Memo::NodeIdSet& _insertedNodeIds;

    /**
     * We own this.
     */
    Memo::NodeTargetGroupMap _targetGroupMap;
};

Memo::Memo(DebugInfo debugInfo, LogicalPropsDeriveFn logicalPropsDeriveFn, CEDeriveFn ceDeriveFn)
    : _groups(),
      _inputGroupsToNodeIdMap(),
      _logicalPropsDeriveFn(std::move(logicalPropsDeriveFn)),
      _ceDeriveFn(std::move(ceDeriveFn)),
      _debugInfo(std::move(debugInfo)) {}

const Group& Memo::getGroup(const GroupIdType groupId) const {
    return _groups.at(groupId);
}

Group& Memo::getGroup(const GroupIdType groupId) {
    return _groups.at(groupId);
}

std::pair<size_t, bool> Memo::findNodeInGroup(GroupIdType groupId, ABT::reference_type node) const {
    return getGroup(groupId)._logicalNodes.find(node);
}

GroupIdType Memo::addGroup(ProjectionNameSet projections) {
    _groups.emplace_back(std::move(projections));
    return _groups.size() - 1;
}

std::pair<Memo::NodeId, bool> Memo::addNode(GroupIdType groupId, ABT n) {
    uassert(6624000, "Attempting to insert a physical node", !n.is<PhysicalNode>());
    uassert(6624000,
            "Attempting to insert a logical delegator node",
            !n.is<MemoLogicalDelegatorNode>());

    OrderPreservingABTSet& nodes = _groups.at(groupId)._logicalNodes;
    auto [index, inserted] = nodes.emplace_back(std::move(n));
    return std::pair<NodeId, bool>({groupId, index}, inserted);
}

ABT::reference_type Memo::getNode(const NodeId nodeMemoId) const {
    return getGroup(nodeMemoId.first)._logicalNodes.at(nodeMemoId.second);
}

std::pair<Memo::NodeId, bool> Memo::findNode(const GroupIdVector& groups, const ABT& node) {
    const auto it = _inputGroupsToNodeIdMap.find(groups);
    if (it != _inputGroupsToNodeIdMap.cend()) {
        for (const NodeId& nodeMemoId : it->second) {
            if (getNode(nodeMemoId) == node) {
                return {nodeMemoId, true};
            }
        }
    }
    return {{0, 0}, false};
}

Memo::NodeId Memo::addNode(GroupIdVector groupVector,
                           ProjectionNameSet projections,
                           const GroupIdType targetGroupId,
                           NodeIdSet& insertedNodeIds,
                           ABT n) {
    auto [existingId, foundNode] = findNode(groupVector, n);

    if (foundNode) {
        uassert(6624000,
                "Found node outside target group",
                targetGroupId < 0 || targetGroupId == existingId.first);
        return existingId;
    }

    // Current node is not in the memo. Insert unchanged.
    const GroupIdType groupId =
        (targetGroupId < 0) ? addGroup(std::move(projections)) : targetGroupId;
    auto [newId, inserted] = addNode(groupId, std::move(n));
    if (inserted) {
        insertedNodeIds.insert(newId);
        _inputGroupsToNodeIdMap[groupVector].insert(newId);

        if (targetGroupId < 0) {
            // If inserted into a new group, derive logical properties, and cardinality estimation
            // for the new group.
            Group& group = getGroup(groupId);
            properties::Properties& props = group._logicalProperties;

            properties::Properties logicalProps = _logicalPropsDeriveFn(*this, groupId);
            props.merge(logicalProps);

            const CEType estimate = _ceDeriveFn(*this, groupId);
            properties::setPropertyOverwrite(props, properties::CardinalityEstimate(estimate));

            if (_debugInfo.hasDebugLevel(2)) {
                std::cout << "Group " << groupId << ": "
                          << ExplainGenerator::explainProperties("Logical properties", props);
            }
        } else if (_debugInfo.isDebugMode()) {
            const Group& group = getGroup(groupId);
            // If inserted into an existing group, verify we deliver all expected projections.
            for (const ProjectionName& groupProjection : group.binder().names()) {
                uassert(6624000,
                        "Node does not project all specified group projections",
                        projections.find(groupProjection) != projections.cend());
            }

            // TODO: possibly verify cardinality estimation
        }
    }

    return newId;
}

GroupIdType Memo::integrate(const ABT& node,
                            NodeTargetGroupMap targetGroupMap,
                            NodeIdSet& insertedNodeIds) {
    MemoIntegrator integrator(*this, std::move(targetGroupMap), insertedNodeIds);
    return integrator.integrate(node);
}

size_t Memo::getGroupCount() const {
    return _groups.size();
}

const Memo::InputGroupsToNodeIdMap& Memo::getInputGroupsToNodeIdMap() const {
    return _inputGroupsToNodeIdMap;
}

const DebugInfo& Memo::getDebugInfo() const {
    return _debugInfo;
}

void Memo::clear() {
    _groups.clear();
    _inputGroupsToNodeIdMap.clear();
}

}  // namespace mongo::optimizer::cascades
