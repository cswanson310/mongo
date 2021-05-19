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

#include "mongo/db/exec/sbe/abt/abt_lower.h"
#include "mongo/db/exec/sbe/stages/bson_scan.h"
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/exchange.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/db/exec/sbe/stages/hash_join.h"
#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/stages/union.h"
#include "mongo/db/exec/sbe/stages/unwind.h"
#include "mongo/db/query/optimizer/utils.h"

namespace mongo::optimizer {
std::unique_ptr<sbe::EExpression> SBEExpressionLowering::optimize(const ABT& n) {
    return algebra::transport<false>(n, *this);
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(const Constant& c) {
    auto [tag, val] = c.get();
    auto [copyTag, copyVal] = sbe::value::copyValue(tag, val);
    sbe::value::ValueGuard guard(copyTag, copyVal);

    auto result = sbe::makeE<sbe::EConstant>(copyTag, copyVal);

    guard.reset();
    return result;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(const Source&) {
    uasserted(0, "not yet implemented");
    return nullptr;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(const Variable& var) {
    auto def = _env.getDefinition(&var);

    if (!def.definedBy.empty()) {
        if (auto let = def.definedBy.cast<Let>(); let) {
            auto it = _letMap.find(let);
            uassert(0, "incorrect let map", it != _letMap.end());

            return sbe::makeE<sbe::EVariable>(it->second, 0, _env.isLastRef(&var));
        } else if (auto lam = def.definedBy.cast<LambdaAbstraction>(); lam) {
            // This is a lambda parameter.
            auto it = _lambdaMap.find(lam);
            uassert(0, "incorrect lambda map", it != _lambdaMap.end());

            return sbe::makeE<sbe::EVariable>(it->second, 0, _env.isLastRef(&var));
        }
    }
    if (auto it = _slotMap.find(var.name()); it != _slotMap.end()) {
        // Found the slot.
        return sbe::makeE<sbe::EVariable>(it->second);
    }
    uasserted(0, "undefined variable");
    return nullptr;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const BinaryOp& op,
    std::unique_ptr<sbe::EExpression> lhs,
    std::unique_ptr<sbe::EExpression> rhs) {

    sbe::EPrimBinary::Op sbeOp = [](const auto abtOp) {
        switch (abtOp) {
            case Operations::Eq:
                return sbe::EPrimBinary::eq;
            case Operations::Neq:
                return sbe::EPrimBinary::neq;
            case Operations::Gt:
                return sbe::EPrimBinary::greater;
            case Operations::Gte:
                return sbe::EPrimBinary::greaterEq;
            case Operations::Lt:
                return sbe::EPrimBinary::less;
            case Operations::Lte:
                return sbe::EPrimBinary::lessEq;
            case Operations::Add:
                return sbe::EPrimBinary::add;
            case Operations::Sub:
                return sbe::EPrimBinary::sub;
            case Operations::And:
                return sbe::EPrimBinary::logicAnd;
            case Operations::Or:
                return sbe::EPrimBinary::logicOr;
            case Operations::Cmp3w:
                return sbe::EPrimBinary::cmp3w;
            default:
                MONGO_UNREACHABLE;
        }
    }(op.op());

    return sbe::makeE<sbe::EPrimBinary>(sbeOp, std::move(lhs), std::move(rhs));
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const UnaryOp& op, std::unique_ptr<sbe::EExpression> arg) {

    sbe::EPrimUnary::Op sbeOp = [](const auto abtOp) {
        switch (abtOp) {
            case Operations::Neg:
                return sbe::EPrimUnary::negate;
            case Operations::Not:
                return sbe::EPrimUnary::logicNot;
            default:
                MONGO_UNREACHABLE;
        }
    }(op.op());

    return sbe::makeE<sbe::EPrimUnary>(sbeOp, std::move(arg));
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const If&,
    std::unique_ptr<sbe::EExpression> cond,
    std::unique_ptr<sbe::EExpression> thenBranch,
    std::unique_ptr<sbe::EExpression> elseBranch) {
    return sbe::makeE<sbe::EIf>(std::move(cond), std::move(thenBranch), std::move(elseBranch));
}

void SBEExpressionLowering::prepare(const Let& let) {
    _letMap[&let] = ++_frameCounter;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const Let& let, std::unique_ptr<sbe::EExpression> bind, std::unique_ptr<sbe::EExpression> in) {
    auto it = _letMap.find(&let);
    uassert(0, "incorrect let map", it != _letMap.end());
    auto frameId = it->second;
    _letMap.erase(it);

    // ABT let binds only a single variable. When we extend it to support multiple binds then we
    // have to revisit how we map variable names to sbe slot ids.
    return sbe::makeE<sbe::ELocalBind>(frameId, sbe::makeEs(std::move(bind)), std::move(in));
}

void SBEExpressionLowering::prepare(const LambdaAbstraction& lam) {
    _lambdaMap[&lam] = ++_frameCounter;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const LambdaAbstraction& lam, std::unique_ptr<sbe::EExpression> body) {
    auto it = _lambdaMap.find(&lam);
    uassert(0, "incorrect lambda map", it != _lambdaMap.end());
    auto frameId = it->second;
    _lambdaMap.erase(it);

    return sbe::makeE<sbe::ELocalLambda>(frameId, std::move(body));
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const LambdaApplication&,
    std::unique_ptr<sbe::EExpression> lam,
    std::unique_ptr<sbe::EExpression> arg) {
    // lambda applications are not directly supported by SBE (yet) and must not be present.
    uasserted(0, "lambda application is not implemented");
    return nullptr;
}

std::unique_ptr<sbe::EExpression> SBEExpressionLowering::transport(
    const FunctionCall& fn, std::vector<std::unique_ptr<sbe::EExpression>> args) {

    // TODO - this is an open question how to do the name mappings.
    auto name = fn.name();
    if (name == "$sum") {
        name = "sum";
    } else if (name == "$first") {
        name = "first";
    }

    return sbe::makeE<sbe::EFunction>(name, std::move(args));
}

sbe::value::SlotVector SBENodeLowering::convertProjectionsToSlots(
    const ProjectionNameVector& projectionNames) {
    sbe::value::SlotVector result;
    for (const ProjectionName& projectionName : projectionNames) {
        auto it = _slotMap.find(projectionName);
        uassert(0, "undefined var", it != _slotMap.end());
        result.push_back(it->second);
    }
    return result;
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::optimize(const ABT& n) {
    return generateInternal(n);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::generateInternal(const ABT& n) {
    return algebra::walk<false>(n, *this);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const RootNode& n,
                                                      const ABT& child,
                                                      const ABT& refs) {

    auto input = generateInternal(child);

    auto output = refs.cast<References>();
    uassert(0, "refs expected", output);

    SlotVarMap finalMap;
    for (auto& o : output->nodes()) {
        auto var = o.cast<Variable>();
        uassert(0, "var expected", var);
        if (auto it = _slotMap.find(var->name()); it != _slotMap.end()) {
            finalMap.emplace(var->name(), it->second);
        }
    }

    _slotMap = finalMap;

    return input;
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const EvaluationNode& n,
                                                      const ABT& child,
                                                      const ABT& binds) {
    auto input = generateInternal(child);

    auto binder = binds.cast<ExpressionBinder>();
    uassert(0, "binder expected", binder);

    auto& names = binder->names();
    auto& exprs = binder->exprs();

    sbe::value::SlotMap<std::unique_ptr<sbe::EExpression>> projects;

    for (size_t idx = 0; idx < exprs.size(); ++idx) {
        auto expr = SBEExpressionLowering{_env, _slotMap}.optimize(exprs[idx]);
        auto slot = _slotIdGenerator.generate();

        _slotMap.emplace(names[idx], slot);
        projects.emplace(slot, std::move(expr));
    }

    return sbe::makeS<sbe::ProjectStage>(std::move(input), std::move(projects), kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const FilterNode& n,
                                                      const ABT& child,
                                                      const ABT& filter) {
    auto input = generateInternal(child);
    auto expr = SBEExpressionLowering{_env, _slotMap}.optimize(filter);

    // Check if the filter expression is 'constant' (i.e. does not depend on any variables)
    // and create FilterStage<true>.
    if (_env.getVariables(filter)._variables.empty()) {
        return sbe::makeS<sbe::FilterStage<true>>(
            std::move(input), std::move(expr), kEmptyPlanNodeId);
    } else {
        return sbe::makeS<sbe::FilterStage<false>>(
            std::move(input), std::move(expr), kEmptyPlanNodeId);
    }
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const LimitSkipNode& n, const ABT& child) {
    auto input = generateInternal(child);

    return sbe::makeS<sbe::LimitSkipStage>(
        std::move(input), n.getProperty().getLimit(), n.getProperty().getSkip(), kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const ExchangeNode& n,
                                                      const ABT& child,
                                                      const ABT& refs) {
    using namespace std::literals;
    // TODO: Implement all types of distributions.
    // TODO: take preserveSort field into account.
    using namespace properties;

    const auto& childProps = getPhysicalProperties(*n.getChild().cast<Node>());
    const auto& childDistribution = getPropertyConst<DistributionRequirement>(childProps);
    uassert(
        0, "Parent and child distributions are the same", !(childDistribution == n.getProperty()));

    const size_t localDOP = (childDistribution.getType() == DistributionType::Centralized)
        ? 1
        : _phaseManager.getMetadata()._numberOfPartitions;
    uassert(0, "invalid DOP", localDOP >= 1);

    auto input = generateInternal(child);

    // Initialized to arbitrary placeholder
    sbe::ExchangePolicy localPolicy = sbe::ExchangePolicy::broadcast;
    std::unique_ptr<sbe::EExpression> partitionExpr;

    switch (n.getProperty().getType()) {
        case DistributionType::Centralized:
        case DistributionType::Replicated:
            localPolicy = sbe::ExchangePolicy::broadcast;
            break;

        case DistributionType::RoundRobin:
            localPolicy = sbe::ExchangePolicy::roundrobin;
            break;

        case DistributionType::RangePartitioning:
            localPolicy = sbe::ExchangePolicy::rangepartition;
            break;

        case DistributionType::HashPartitioning: {
            localPolicy = sbe::ExchangePolicy::hashpartition;
            std::vector<std::unique_ptr<sbe::EExpression>> args;
            for (const auto& proj : n.getProperty().getProjections()) {
                auto it = _slotMap.find(proj);
                uassert(0, "undefined var", it != _slotMap.end());

                args.emplace_back(sbe::makeE<sbe::EVariable>(it->second));
            }
            partitionExpr = sbe::makeE<sbe::EFunction>("hash"_sd, std::move(args));
            break;
        }

        case DistributionType::UnknownPartitioning:
            uasserted(0, "Cannot partition into unknown distribution");

        default:
            MONGO_UNREACHABLE;
    }

    sbe::value::SlotVector fieldSlots;
    {
        const auto& props = getPhysicalProperties(n);
        const auto& requiredProjections =
            getPropertyConst<ProjectionRequirement>(props).getProjections();

        for (auto& o : requiredProjections.getVector()) {
            auto it1 = _slotMap.find(o);
            uassert(0, "undefined var", it1 != _slotMap.end());
            fieldSlots.push_back(it1->second);
        }
    }

    return sbe::makeS<sbe::ExchangeConsumer>(std::move(input),
                                             localDOP,
                                             std::move(fieldSlots),
                                             localPolicy,
                                             std::move(partitionExpr),
                                             nullptr,
                                             kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const CollationNode& n,
                                                      const ABT& child,
                                                      const ABT& refs) {
    auto input = generateInternal(child);

    sbe::value::SlotVector orderBySlots;
    std::vector<sbe::value::SortDirection> directions;
    ProjectionNameSet collationProjections;
    for (const auto& collationEntry : n.getProperty().getCollationSpec()) {
        collationProjections.insert(collationEntry.first);
        auto it = _slotMap.find(collationEntry.first);

        uassert(0, "undefined orderBy var", it != _slotMap.end());
        orderBySlots.push_back(it->second);

        switch (collationEntry.second) {
            case CollationOp::Ascending:
            case CollationOp::Clustered:
                // TODO: is there a more efficient way to compute clustered collation op than sort?
                directions.push_back(sbe::value::SortDirection::Ascending);
                break;

            case CollationOp::Descending:
                directions.push_back(sbe::value::SortDirection::Descending);
                break;

            default:
                MONGO_UNREACHABLE;
        }
    }

    sbe::value::SlotVector outputSlots;

    const auto& physProps = getPhysicalProperties(n);
    const auto& requiredProjections =
        properties::getPropertyConst<properties::ProjectionRequirement>(physProps).getProjections();
    for (const auto& requiredProjection : requiredProjections.getVector()) {
        if (collationProjections.count(requiredProjection) == 0) {
            auto it = _slotMap.find(requiredProjection);
            uassert(0, "undefined output var", it != _slotMap.end());
            outputSlots.push_back(it->second);
        }
    }

    size_t limit = std::numeric_limits<std::size_t>::max();
    if (properties::hasProperty<properties::LimitSkipRequirement>(physProps)) {
        limit = properties::getPropertyConst<properties::LimitSkipRequirement>(physProps)
                    .getAbsoluteLimit();
    }

    // TODO: obtain defaults for these.
    const size_t memoryLimit = 100 * (1ul << 20);  // 100MB
    const bool allowDiskUse = false;

    return sbe::makeS<sbe::SortStage>(std::move(input),
                                      std::move(orderBySlots),
                                      std::move(directions),
                                      std::move(outputSlots),
                                      limit,
                                      memoryLimit,
                                      allowDiskUse,
                                      kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const GroupByNode& n,
                                                      const ABT& child,
                                                      const ABT& aggBinds,
                                                      const ABT& aggRefs,
                                                      const ABT& gbBind,
                                                      const ABT& gbRefs) {

    auto input = generateInternal(child);

    // Ideally, we should make a distinction between gbBind and gbRefs; i.e. internal references
    // used by the hash agg to determinte the group by values from its input and group by values as
    // outputted by the hash agg after the grouping. However, SBE hash agg uses the same slot it to
    // represent both so that distinction is kind of moot.
    sbe::value::SlotVector gbs;
    auto gbCols = gbRefs.cast<References>();
    uassert(0, "refs expected", gbCols);
    for (auto& o : gbCols->nodes()) {
        auto var = o.cast<Variable>();
        uassert(0, "var expected", var);
        auto it = _slotMap.find(var->name());
        uassert(0, "undefined var", it != _slotMap.end());
        gbs.push_back(it->second);
    }

    // Similar considerations apply to the agg expressions as to the group by columns.
    auto binderAgg = aggBinds.cast<ExpressionBinder>();
    uassert(0, "binder expected", binderAgg);
    auto refsAgg = aggRefs.cast<References>();
    uassert(0, "refs expected", refsAgg);

    auto& names = binderAgg->names();
    auto& exprs = refsAgg->nodes();

    sbe::value::SlotMap<std::unique_ptr<sbe::EExpression>> aggs;

    for (size_t idx = 0; idx < exprs.size(); ++idx) {
        auto expr = SBEExpressionLowering{_env, _slotMap}.optimize(exprs[idx]);
        auto slot = _slotIdGenerator.generate();

        _slotMap.emplace(names[idx], slot);
        aggs.emplace(slot, std::move(expr));
    }

    // TODO: use collator slot.
    boost::optional<sbe::value::SlotId> collatorSlot;
    return sbe::makeS<sbe::HashAggStage>(
        std::move(input), std::move(gbs), std::move(aggs), collatorSlot, kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const BinaryJoinNode& n,
                                                      const ABT& leftChild,
                                                      const ABT& rightChild,
                                                      const ABT& filter) {
    auto outerStage = generateInternal(leftChild);
    auto innerStage = generateInternal(rightChild);

    // List of correlated projections (bound in outer side and referred to in the inner side).
    sbe::value::SlotVector correlatedSlots;
    for (const ProjectionName& projectionName : n.getCorrelatedProjectionNames()) {
        correlatedSlots.push_back(_slotMap.at(projectionName));
    }

    // Out of all required slots which are coming from outer side (others implicitly from inner
    // side).
    sbe::value::SlotVector outerSlots;
    for (auto& varName : collectVariableReferences(leftChild)) {
        outerSlots.push_back(_slotMap.at(varName));
    }

    auto expr = SBEExpressionLowering{_env, _slotMap}.optimize(filter);

    return sbe::makeS<sbe::LoopJoinStage>(std::move(outerStage),
                                          std::move(innerStage),
                                          std::move(outerSlots),
                                          std::move(correlatedSlots),
                                          std::move(expr),
                                          kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const HashJoinNode& n,
                                                      const ABT& leftChild,
                                                      const ABT& rightChild,
                                                      const ABT& refs) {
    auto outerStage = generateInternal(leftChild);
    auto innerStage = generateInternal(rightChild);

    uassert(0, "Only inner joins supported for now", n.getJoinType() == JoinType::Inner);

    const auto convertChildProjectionsToSlots = [this](const ABT& child) {
        const auto& childProps = getPhysicalProperties(*child.cast<Node>());
        const auto& requiredProjections =
            properties::getPropertyConst<properties::ProjectionRequirement>(childProps)
                .getProjections();
        return convertProjectionsToSlots(requiredProjections.getVector());
    };

    // TODO: use collator slot.
    boost::optional<sbe::value::SlotId> collatorSlot;
    return sbe::makeS<sbe::HashJoinStage>(std::move(outerStage),
                                          std::move(innerStage),
                                          convertProjectionsToSlots(n.getLeftKeys()),
                                          convertChildProjectionsToSlots(n.getLeftChild()),
                                          convertProjectionsToSlots(n.getRightKeys()),
                                          convertChildProjectionsToSlots(n.getRightChild()),
                                          collatorSlot,
                                          kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const UnionNode& n,
                                                      const ABTVector& children,
                                                      const ABT& binder,
                                                      const ABT& refs) {
    auto unionBinder = binder.cast<ExpressionBinder>();
    uassert(0, "binder expected", unionBinder);
    const auto& names = unionBinder->names();

    std::vector<std::unique_ptr<sbe::PlanStage>> loweredChildren;
    std::vector<sbe::value::SlotVector> inputVals;

    for (const ABT& child : children) {
        // Use a fresh map to prevent same projections for every child being overwritten.
        SlotVarMap localMap;
        SBENodeLowering localLowering(_env, localMap, _slotIdGenerator, _phaseManager, _randomScan);
        loweredChildren.emplace_back(localLowering.optimize(child));

        sbe::value::SlotVector childSlots;
        for (const auto& name : names) {
            childSlots.push_back(localMap.at(name));
        }
        inputVals.emplace_back(std::move(childSlots));
    }

    sbe::value::SlotVector outputVals;
    for (const auto& name : names) {
        const auto outputSlot = _slotIdGenerator.generate();
        _slotMap.emplace(name, outputSlot);
        outputVals.push_back(outputSlot);
    }

    return sbe::makeS<sbe::UnionStage>(
        std::move(loweredChildren), std::move(inputVals), std::move(outputVals), kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const UnwindNode& n,
                                                      const ABT& child,
                                                      const ABT& pidBind,
                                                      const ABT& refs) {
    auto input = generateInternal(child);

    auto it = _slotMap.find(n.getProjectionName());
    uassert(0, "undefined unwind field", it != _slotMap.end());

    auto inputSlot = it->second;
    auto outputSlot = _slotIdGenerator.generate();
    auto outputPidSlot = _slotIdGenerator.generate();

    _slotMap[n.getProjectionName()] = outputSlot;
    _slotMap[n.getPIDProjectionName()] = outputPidSlot;

    return sbe::makeS<sbe::UnwindStage>(std::move(input),
                                        inputSlot,
                                        outputSlot,
                                        outputPidSlot,
                                        n.getRetainNonArrays(),
                                        kEmptyPlanNodeId);
}

void SBENodeLowering::generateSlots(const FieldProjectionMap& fieldProjectionMap,
                                    boost::optional<sbe::value::SlotId>& ridSlot,
                                    boost::optional<sbe::value::SlotId>& rootSlot,
                                    std::vector<std::string>& fields,
                                    sbe::value::SlotVector& vars) {
    if (!fieldProjectionMap._ridProjection.empty()) {
        ridSlot = _slotIdGenerator.generate();
        _slotMap.emplace(fieldProjectionMap._ridProjection, ridSlot.value());
    }
    if (!fieldProjectionMap._rootProjection.empty()) {
        rootSlot = _slotIdGenerator.generate();
        _slotMap.emplace(fieldProjectionMap._rootProjection, rootSlot.value());
    }
    for (const auto& [fieldName, projectionName] : fieldProjectionMap._fieldProjections) {
        vars.push_back(_slotIdGenerator.generate());
        _slotMap.emplace(projectionName, vars.back());
        fields.push_back(fieldName);
    }
}

static NamespaceStringOrUUID parseFromScanDef(const ScanDefinition& def) {
    const auto& dbName = def.getOptionsMap().at("database");
    const auto& uuidStr = def.getOptionsMap().at("uuid");
    return {dbName, UUID::parse(uuidStr).getValue()};
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::lowerScanNode(
    const Node& n,
    const std::string& scanDefName,
    const FieldProjectionMap& fieldProjectionMap,
    const bool useParallelScan) {
    const ScanDefinition& def = _phaseManager.getMetadata()._scanDefs.at(scanDefName);
    auto& typeSpec = def.getOptionsMap().at("type");

    boost::optional<sbe::value::SlotId> ridSlot;
    boost::optional<sbe::value::SlotId> rootSlot;
    std::vector<std::string> fields;
    sbe::value::SlotVector vars;
    generateSlots(fieldProjectionMap, ridSlot, rootSlot, fields, vars);

    if (typeSpec == "mongod") {
        NamespaceStringOrUUID nss = parseFromScanDef(def);

        // Unused.
        boost::optional<sbe::value::SlotId> seekKeySlot;

        sbe::ScanCallbacks callbacks({}, {}, {}, {});
        if (useParallelScan) {
            return sbe::makeS<sbe::ParallelScanStage>(nss.uuid().get(),
                                                      rootSlot,
                                                      ridSlot,
                                                      boost::none,
                                                      boost::none,
                                                      boost::none,
                                                      boost::none,
                                                      fields,
                                                      vars,
                                                      nullptr /*yieldPolicy*/,
                                                      kEmptyPlanNodeId,
                                                      callbacks);
        } else {
            return sbe::makeS<sbe::ScanStage>(nss.uuid().get(),
                                              rootSlot,
                                              ridSlot,
                                              boost::none,
                                              boost::none,
                                              boost::none,
                                              boost::none,
                                              boost::none,
                                              fields,
                                              vars,
                                              seekKeySlot,
                                              true /*forward*/,
                                              nullptr /*yieldPolicy*/,
                                              kEmptyPlanNodeId,
                                              callbacks,
                                              _randomScan);
        }
    }

    if (typeSpec == "test") {
        const auto makeNothingFn = []() {
            return sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::Nothing, 0);
        };

        sbe::value::SlotMap<std::unique_ptr<sbe::EExpression>> projects;
        if (rootSlot.has_value()) {
            projects.emplace(rootSlot.value(), makeNothingFn());
        }
        for (const auto slot : vars) {
            projects.emplace(slot, makeNothingFn());
        }

        return sbe::makeS<sbe::ProjectStage>(
            sbe::makeS<sbe::LimitSkipStage>(
                sbe::makeS<sbe::CoScanStage>(kEmptyPlanNodeId), 1, 0, kEmptyPlanNodeId),
            std::move(projects),
            kEmptyPlanNodeId);
    }

    if (typeSpec == "bson") {
        const auto& filename = def.getOptionsMap().at("fileName");
        if (useParallelScan) {
            return sbe::makeS<sbe::ParallelBsonScanStage>(
                filename, rootSlot, fields, vars, kEmptyPlanNodeId);
        } else {
            return sbe::makeS<sbe::BSONScanStage>(
                filename, rootSlot, fields, vars, kEmptyPlanNodeId);
        }
    }

    if (typeSpec == "bsonStdin") {
        return sbe::makeS<sbe::BSONScanStdinStage>(rootSlot, fields, vars, kEmptyPlanNodeId);
    }
    return nullptr;
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const ScanNode& n, const ABT& /*binds*/) {
    FieldProjectionMap fieldProjectionMap;
    fieldProjectionMap._rootProjection = n.getProjectionName();
    return lowerScanNode(n, n.getScanDefName(), fieldProjectionMap, false /*useParallelScan*/);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const PhysicalScanNode& n,
                                                      const ABT& /*binds*/) {
    return lowerScanNode(n, n.getScanDefName(), n.getFieldProjectionMap(), n.useParallelScan());
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const CoScanNode& n) {
    return sbe::makeS<sbe::CoScanStage>(kEmptyPlanNodeId);
}

std::unique_ptr<sbe::EExpression> SBENodeLowering::convertBoundsToExpr(
    const bool isLower,
    const IndexDefinition& indexDef,
    const MultiKeyIntervalRequirement& interval) {
    std::vector<std::unique_ptr<sbe::EExpression>> ksFnArgs;
    ksFnArgs.emplace_back(
        sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::NumberInt64,
                                   sbe::value::bitcastFrom<int64_t>(indexDef.getVersion())));

    // TODO: ordering is unsigned int32??
    ksFnArgs.emplace_back(
        sbe::makeE<sbe::EConstant>(sbe::value::TypeTags::NumberInt32,
                                   sbe::value::bitcastFrom<uint32_t>(indexDef.getOrdering())));

    auto exprLower = SBEExpressionLowering{_env, _slotMap};
    bool inclusive = true;
    bool fullyInfinite = true;
    for (const auto& entry : interval) {
        const BoundRequirement& entryBound = isLower ? entry.getLowBound() : entry.getHighBound();
        const bool isInfinite = entryBound.isInfinite();
        if (!isInfinite) {
            fullyInfinite = false;
            if (!entryBound.isInclusive()) {
                inclusive = false;
            }
        }

        ABT bound = isInfinite ? (isLower ? Constant::minKey() : Constant::maxKey())
                               : entryBound.getBound();
        auto boundExpr = exprLower.optimize(std::move(bound));
        ksFnArgs.emplace_back(std::move(boundExpr));
    }
    if (fullyInfinite && !isLower) {
        // We can skip if fully infinite only for upper bound. For lower bound we need to generate
        // minkeys.
        return nullptr;
    };

    ksFnArgs.emplace_back(sbe::makeE<sbe::EConstant>(
        sbe::value::TypeTags::NumberInt64,
        sbe::value::bitcastFrom<int64_t>((isLower == inclusive) ? 1 : 2)));
    return sbe::makeE<sbe::EFunction>("ks", std::move(ksFnArgs));
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const IndexScanNode& n, const ABT&) {
    const auto& fieldProjectionMap = n.getFieldProjectionMap();
    const auto& indexSpec = n.getIndexSpecification();

    const auto& intervals = indexSpec.getIntervals();
    uassert(0,
            "Can only lower singular intervals",
            intervals.size() == 1 && intervals.at(0).size() == 1);
    const auto& interval = intervals.at(0).at(0);

    const std::string& indexDefName = n.getIndexSpecification().getIndexDefName();
    const auto& metadata = _phaseManager.getMetadata();
    const ScanDefinition& scanDef = metadata._scanDefs.at(indexSpec.getScanDefName());
    const IndexDefinition& indexDef = scanDef.getIndexDefs().at(indexDefName);

    NamespaceStringOrUUID nss = parseFromScanDef(scanDef);

    boost::optional<sbe::value::SlotId> ridSlot;
    boost::optional<sbe::value::SlotId> rootSlot;
    std::vector<std::string> fields;
    sbe::value::SlotVector vars;
    generateSlots(fieldProjectionMap, ridSlot, rootSlot, fields, vars);
    uassert(0, "Cannot deliver root projection in this context", !rootSlot.has_value());

    sbe::IndexKeysInclusionSet indexKeysToInclude;
    for (const std::string& fieldName : fields) {
        std::istringstream is(fieldName);

        std::string prefix;
        is >> prefix;
        uassert(0, "Invalid index key prefix", prefix == kIndexKeyPrefix);

        int key;
        is >> key;

        indexKeysToInclude.set(key, true);
    }

    auto lowerBoundExpr = convertBoundsToExpr(true /*isLower*/, indexDef, interval);
    auto upperBoundExpr = convertBoundsToExpr(false /*isLower*/, indexDef, interval);
    const bool hasLowerBound = lowerBoundExpr != nullptr;
    const bool hasUpperBound = upperBoundExpr != nullptr;
    uassert(0, "Invalid bounds combination", hasLowerBound || !hasUpperBound);

    boost::optional<sbe::value::SlotId> seekKeySlotLower;
    boost::optional<sbe::value::SlotId> seekKeySlotUpper;
    sbe::value::SlotVector correlatedSlotsForJoin;

    auto projectForKeyStringBounds = sbe::makeS<sbe::LimitSkipStage>(
        sbe::makeS<sbe::CoScanStage>(kEmptyPlanNodeId), 1, boost::none, kEmptyPlanNodeId);
    if (hasLowerBound) {
        seekKeySlotLower = _slotIdGenerator.generate();
        correlatedSlotsForJoin.push_back(seekKeySlotLower.value());
        projectForKeyStringBounds = sbe::makeProjectStage(std::move(projectForKeyStringBounds),
                                                          kEmptyPlanNodeId,
                                                          seekKeySlotLower.value(),
                                                          std::move(lowerBoundExpr));
    }
    if (hasUpperBound) {
        seekKeySlotUpper = _slotIdGenerator.generate();
        correlatedSlotsForJoin.push_back(seekKeySlotUpper.value());
        projectForKeyStringBounds = sbe::makeProjectStage(std::move(projectForKeyStringBounds),
                                                          kEmptyPlanNodeId,
                                                          seekKeySlotUpper.value(),
                                                          std::move(upperBoundExpr));
    }

    // Unused.
    boost::optional<sbe::value::SlotId> resultSlot;

    auto indexStage = sbe::makeS<sbe::IndexScanStage>(nss.uuid().get(),
                                                      indexDefName,
                                                      !indexSpec.isReverseOrder(),
                                                      resultSlot,
                                                      ridSlot,
                                                      boost::none,
                                                      indexKeysToInclude,
                                                      vars,
                                                      seekKeySlotLower,
                                                      seekKeySlotUpper,
                                                      nullptr /*yieldPolicy*/,
                                                      kEmptyPlanNodeId,
                                                      sbe::LockAcquisitionCallback{});

    return sbe::makeS<sbe::LoopJoinStage>(std::move(projectForKeyStringBounds),
                                          std::move(indexStage),
                                          sbe::makeSV(),
                                          std::move(correlatedSlotsForJoin),
                                          nullptr,
                                          kEmptyPlanNodeId);
}

std::unique_ptr<sbe::PlanStage> SBENodeLowering::walk(const SeekNode& n,
                                                      const ABT& /*binds*/,
                                                      const ABT& /*refs*/) {
    const ScanDefinition& def = _phaseManager.getMetadata()._scanDefs.at(n.getScanDefName());

    auto& typeSpec = def.getOptionsMap().at("type");
    uassert(0, "SeekNode only supports mongod collections", typeSpec == "mongod");
    NamespaceStringOrUUID nss = parseFromScanDef(def);

    boost::optional<sbe::value::SlotId> ridSlot;
    boost::optional<sbe::value::SlotId> rootSlot;
    std::vector<std::string> fields;
    sbe::value::SlotVector vars;
    generateSlots(n.getFieldProjectionMap(), ridSlot, rootSlot, fields, vars);

    boost::optional<sbe::value::SlotId> seekKeySlot = _slotMap.at(n.getRIDProjectionName());

    sbe::ScanCallbacks callbacks({}, {}, {}, {});
    auto seekStage = sbe::makeS<sbe::ScanStage>(nss.uuid().get(),
                                                rootSlot,
                                                ridSlot,
                                                boost::none,
                                                boost::none,
                                                boost::none,
                                                boost::none,
                                                boost::none,
                                                fields,
                                                vars,
                                                seekKeySlot,
                                                true /*forward*/,
                                                nullptr /*yieldPolicy*/,
                                                kEmptyPlanNodeId,
                                                callbacks);

    return sbe::makeS<sbe::LimitSkipStage>(
        std::move(seekStage), 1 /*limit*/, 0 /*skip*/, kEmptyPlanNodeId);
}

const properties::Properties& SBENodeLowering::getPhysicalProperties(const Node& n) const {
    const auto& nodeToPhysPropsMap = _phaseManager.getNodeToPhysPropsMap();
    auto it = nodeToPhysPropsMap.find(&n);
    uassert(0, "Cannot find physical properties for node.", it != nodeToPhysPropsMap.cend());

    const MemoPhysicalNodeId id = it->second;
    const Memo& memo = _phaseManager.getMemo();
    return memo.getGroup(id._groupId)._physicalNodes.at(id._index)->_physProps;
}

}  // namespace mongo::optimizer
