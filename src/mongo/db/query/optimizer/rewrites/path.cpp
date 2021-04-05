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

#include "mongo/db/query/optimizer/rewrites/path.h"

namespace mongo::optimizer {
ABT::reference_type PathFusion::follow(ABT::reference_type n) {
    if (auto var = n.cast<Variable>(); var) {
        auto def = _env.getDefinition(var);
        if (!def.definition.empty() && !def.definition.is<Source>()) {
            return follow(def.definition);
        }
    }

    return n;
}

bool PathFusion::fuse(ABT& lhs, ABT& rhs) {
    auto lhsGet = lhs.cast<PathGet>();
    auto rhsField = rhs.cast<PathField>();
    if (lhsGet && rhsField && lhsGet->name() == rhsField->name()) {
        return fuse(lhsGet->getPath(), rhsField->getPath());
    }

    auto lhsTraverse = lhs.cast<PathTraverse>();
    auto rhsTraverse = rhs.cast<PathTraverse>();
    if (lhsTraverse && rhsTraverse) {
        return fuse(lhsTraverse->getPath(), rhsTraverse->getPath());
    }

    if (lhsTraverse) {
        auto rhsType = _info[rhs.cast<PathSyntaxSort>()].type;
        if (rhsType != Type::unknown && rhsType != Type::array) {
            auto result = std::exchange(lhsTraverse->getPath(), make<Blackhole>());
            std::swap(lhs, result);
            return fuse(lhs, rhs);
        }
    }

    if (lhs.is<PathIdentity>()) {
        lhs = rhs;
        return true;
    }

    if (rhs.is<PathLambda>()) {
        lhs = make<PathComposeM>(std::move(lhs), rhs);
        return true;
    }
    auto cmp = lhs.cast<PathCompare>();
    auto constant = rhs.cast<PathConstant>();
    if (cmp && constant) {
        auto op = make<BinaryOp>(
            cmp->op(),
            make<BinaryOp>(Operations::Cmp3w, constant->getConstant(), cmp->getVal()),
            Constant::int64(0));

        auto result = make<PathConstant>(std::move(op));

        std::swap(lhs, result);
        std::cout << "huuuaaaaa\n";

        return true;
    }
    return false;
}

bool PathFusion::optimize(ABT& root) {
    _changed = false;
    algebra::transport<true>(root, *this);

    if (_changed) {
        _env.rebuild(root);
    }

    return _changed;
}

void PathFusion::transport(ABT& n, const PathConstant& path, ABT& c) {
    CollectedInfo ci;
    if (auto exprC = path.getConstant().cast<Constant>(); exprC) {
        // Let's see what we can determine from the constant expression
        auto [tag, val] = exprC->get();
        if (sbe::value::isObject(tag)) {
            ci.type = Type::object;
        } else if (sbe::value::isArray(tag)) {
            ci.type = Type::array;
        } else if (tag == sbe::value::TypeTags::Boolean) {
            ci.type = Type::boolean;
        } else if (tag == sbe::value::TypeTags::Nothing) {
            ci.type = Type::nothing;
        } else {
            ci.type = Type::any;
        }
    }
    _info[&path] = ci;
}

void PathFusion::transport(ABT& n, const PathCompare& path, ABT& c) {
    CollectedInfo ci;

    // TODO - follow up on Nothing and 3 value logic. Assume plain boolean for now.
    ci.type = Type::boolean;

    _info[&path] = ci;
}

void PathFusion::transport(ABT& n, const PathGet& get, ABT& path) {
    // Get "a" Const <c> -> Const <c>
    if (auto constPath = path.cast<PathConstant>(); constPath) {
        // Pull out the constant path
        auto result = std::exchange(path, make<Blackhole>());

        // And swap it for the current node
        std::swap(n, result);

        _changed = true;
    } else {
        auto it = _info.find(path.cast<PathSyntaxSort>());
        uassert(0, "expected to find path", it != _info.end());

        // Simply move the info from the child.
        _info[&get] = it->second;
    }
}

void PathFusion::transport(ABT& n, const PathField& field, ABT& path) {
    auto it = _info.find(path.cast<PathSyntaxSort>());
    uassert(0, "expected to find path", it != _info.end());

    CollectedInfo ci;
    if (it->second.type == Type::unknown) {
        // We don't know anything about the child.
        ci.type = Type::unknown;
    } else if (it->second.type == Type::nothing) {
        // We are setting a field to nothing (aka drop) hence we do not know what the result could
        // be (i.e. it all depends on the input).
        ci.type = Type::unknown;
    } else {
        // The child produces bona fide value hence this will become an object.
        ci.type = Type::object;
    }

    _info[&field] = ci;
}

void PathFusion::transport(ABT& n, const PathTraverse& path, ABT& inner) {
    // Traverse is completely dependent on its input and we cannot say anything about it.
    CollectedInfo ci;
    ci.type = Type::unknown;
    _info[&path] = ci;
}

static std::vector<ABT::reference_type> collectComposed(const ABT& n) {
    if (auto comp = n.cast<PathComposeM>(); comp) {
        auto lhs = collectComposed(comp->getPath1());
        auto rhs = collectComposed(comp->getPath2());
        lhs.insert(lhs.end(), rhs.begin(), rhs.end());

        return lhs;
    }

    return {n.ref()};
}

void PathFusion::transport(ABT& n, const PathComposeM& path, ABT& p1, ABT& p2) {
    if (p1.is<PathIdentity>()) {
        // Id * p2 -> p2
        auto result = std::exchange(p2, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
        return;
    } else if (p2.is<PathIdentity>()) {
        // p1 * Id -> p1
        auto result = std::exchange(p1, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
        return;
    } else if (_redundant.erase(p1.cast<PathSyntaxSort>())) {
        auto result = std::exchange(p2, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
        return;
    } else if (_redundant.erase(p2.cast<PathSyntaxSort>())) {
        auto result = std::exchange(p1, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
        return;
    }

    auto p1InfoIt = _info.find(p1.cast<PathSyntaxSort>());
    auto p2InfoIt = _info.find(p2.cast<PathSyntaxSort>());

    uassert(0, "info must be defined", p1InfoIt != _info.end() && p2InfoIt != _info.end());

    if (p1.is<PathDefault>() && p2InfoIt->second.isNotNothing()) {
        // Default * Const e -> e (provided we can prove e is not Nothing and we can do that only
        // when e is Constant expression)
        auto result = std::exchange(p2, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
    } else if (p2.is<PathDefault>() && p1InfoIt->second.isNotNothing()) {
        // Const e * Default -> e (provided we can prove e is not Nothing and we can do that only
        // when e is Constant expression)
        auto result = std::exchange(p1, make<Blackhole>());
        std::swap(n, result);
        _changed = true;
    } else if (p2InfoIt->second.type == Type::object) {
        auto left = collectComposed(p1);
        for (auto l : left) {
            if (l.is<PathObj>()) {
                _redundant.emplace(l.cast<PathSyntaxSort>());
                _changed = true;
            }
        }
        _info[&path] = p2InfoIt->second;
    } else {
        _info[&path] = p2InfoIt->second;
    }
}

void PathFusion::transport(ABT& n, const EvalPath& eval, ABT& path, ABT& input) {
    auto realInput = follow(input);
    // If we are evaluating const path then we can simply replace the whole expression with the
    // result.
    if (auto constPath = path.cast<PathConstant>(); constPath) {
        // Pull out the constant out of the path
        auto result = std::exchange(constPath->getConstant(), make<Blackhole>());

        // And swap it for the current node
        std::swap(n, result);

        _changed = true;
    } else if (auto evalInput = realInput.cast<EvalPath>(); evalInput) {
        // An input to 'this' EvalPath expression is another EvalPath so we may try to fuse the
        // paths.
        if (fuse(n.cast<EvalPath>()->getPath(), evalInput->getPath())) {
            // We have fusef paths so replace the input (by making a copy).
            input = evalInput->getInput();

            _changed = true;
        }
    }
    _kindCtx.pop_back();
}

void PathFusion::transport(ABT& n, const EvalFilter& eval, ABT& path, ABT& input) {
    auto realInput = follow(input);
    // If we are evaluating const path then we can simply replace the whole expression with the
    // result.
    if (auto constPath = path.cast<PathConstant>(); constPath) {
        // Pull out the constant out of the path
        auto result = std::exchange(constPath->getConstant(), make<Blackhole>());

        // And swap it for the current node
        std::swap(n, result);

        _changed = true;
    } else if (auto evalInput = realInput.cast<EvalPath>(); evalInput) {
        // An input to 'this' EvalFilter expression is another EvalPath so we may try to fuse the
        // paths.
        if (fuse(n.cast<EvalFilter>()->getPath(), evalInput->getPath())) {
            // We have fusef paths so replace the input (by making a copy).
            input = evalInput->getInput();

            _changed = true;
        }
    }
    _kindCtx.pop_back();
}
}  // namespace mongo::optimizer
