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

#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mongo/db/query/optimizer/printable_enum.h"


namespace mongo::optimizer {

using FieldNameType = std::string;
using FieldPathType = std::vector<FieldNameType>;

using CollectionNameType = std::string;

using ProjectionName = std::string;
using ProjectionNameSet = std::unordered_set<ProjectionName>;
using ProjectionNameOrderedSet = std::set<ProjectionName>;
using ProjectionNameVector = std::vector<ProjectionName>;

class ProjectionNameOrderPreservingSet {
public:
    ProjectionNameOrderPreservingSet() = default;
    ProjectionNameOrderPreservingSet(ProjectionNameVector v);

    ProjectionNameOrderPreservingSet(const ProjectionNameOrderPreservingSet& other);
    ProjectionNameOrderPreservingSet(ProjectionNameOrderPreservingSet&& other) noexcept;

    bool operator==(const ProjectionNameOrderPreservingSet& other) const;

    std::pair<size_t, bool> emplace_back(ProjectionName projectionName);
    std::pair<size_t, bool> find(const ProjectionName& projectionName) const;
    bool erase(const ProjectionName& projectionName);

    const ProjectionNameVector& getVector() const;

private:
    std::unordered_map<ProjectionName, size_t> _map;
    ProjectionNameVector _vector;
};

using GroupIdType = int64_t;

#define DISTRIBUTIONTYPE_NAMES(F) \
    F(Centralized)                \
    F(Replicated)                 \
    F(RoundRobin)                 \
    F(HashPartitioning)           \
    F(RangePartitioning)          \
    F(UnknownPartitioning)

MAKE_PRINTABLE_ENUM(DistributionType, DISTRIBUTIONTYPE_NAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(DistributionTypeEnum, DistributionType, DISTRIBUTIONTYPE_NAMES);
#undef DISTRIBUTIONTYPE_NAMES

// In case of covering scan, index, or fetch, specify names of bound projections for each field.
// Also optionally specify if applicable the rid and record (root) projections.
struct FieldProjectionMap {
    ProjectionName _ridProjection;
    ProjectionName _rootProjection;
    std::unordered_map<FieldNameType, ProjectionName> _fieldProjections;

    bool operator==(const FieldProjectionMap& other) const {
        return _ridProjection == other._ridProjection && _rootProjection == other._rootProjection &&
            _fieldProjections == other._fieldProjections;
    }
};

// Used to generate field names encoding index keys for covered indexes.
static constexpr const char* kIndexKeyPrefix = "<indexKey>";

struct MemoPhysicalNodeId {
    GroupIdType _groupId;
    size_t _index;

    bool operator==(const MemoPhysicalNodeId& other) const {
        return _groupId == other._groupId && _index == other._index;
    }
};

class DebugInfo {
public:
    static const int kIterationLimitForTests = 10000;
    static const int kDefaultDebugLevelForTests = 1;

    static DebugInfo kDefaultForTests;
    static DebugInfo kDefaultForProd;

    DebugInfo(const bool debugMode, const int debugLevel, const int iterationLimit);

    bool isDebugMode() const;

    bool hasDebugLevel(const int debugLevel) const;

    bool exceedsIterationLimit(const int iterations) const;

private:
    // Are we in debug mode? Can we do additional logging, etc?
    const bool _debugMode;

    const int _debugLevel;

    // Maximum number of rewrite iterations.
    const int _iterationLimit;
};

using CEType = double;
using SelectivityType = double;

class CostType {
public:
    static CostType kInfinity;
    static CostType kZero;

    static CostType fromDouble(const double cost);

    CostType& operator=(const CostType& other);

    bool operator==(const CostType& other) const;
    bool operator!=(const CostType& other) const;
    bool operator<(const CostType& other) const;

    CostType operator+(const CostType& other) const;
    CostType operator-(const CostType& other) const;
    CostType& operator+=(const CostType& other);

    std::string toString() const;

private:
    CostType(const bool isInfinite, const double cost);

    bool _isInfinite;
    double _cost;
};

#define COLLATIONOP_OPNAMES(F) \
    F(Ascending)               \
    F(Descending)              \
    F(Clustered)

MAKE_PRINTABLE_ENUM(CollationOp, COLLATIONOP_OPNAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(CollationOpEnum, CollationOp, COLLATIONOP_OPNAMES);
#undef PATHSYNTAX_OPNAMES

using ProjectionCollationEntry = std::pair<ProjectionName, CollationOp>;
using ProjectionCollationSpec = std::vector<ProjectionCollationEntry>;

CollationOp reverseCollationOp(const CollationOp op);

bool collationOpsCompatible(const CollationOp availableOp, const CollationOp requiredOp);
bool collationsCompatible(const ProjectionCollationSpec& available,
                          const ProjectionCollationSpec& required);

}  // namespace mongo::optimizer
