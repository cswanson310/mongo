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

#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/cascades/memo.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/util/assert_util.h"

namespace mongo::optimizer {

template <int version = 1>
class ExplainGeneratorTransporter {

    struct MarkChildCount {
        MarkChildCount(const int childCount) : _childCount(childCount) {}
        int _childCount;
    };

    class ExplainPrinter {
        enum class CommandType { Indent, Unindent, AddLine };

        struct CommandStruct {
            CommandStruct(const CommandType type, std::string str)
                : _type(type), _str(std::move(str)) {}

            CommandType _type;
            std::string _str;
        };


        using CommandVector = std::vector<CommandStruct>;

    public:
        ExplainPrinter() : _cmd(), _os(), _osDirty(false), _indentCount(0), _childrenRemaining(0) {}

        ~ExplainPrinter() {
            uassert(6624099, "Unmatched indentations", _indentCount == 0);
            uassert(6624099, "Incorrect child count mark", _childrenRemaining == 0);
        }

        explicit ExplainPrinter(const std::string& initialStr) : ExplainPrinter() {
            *this << initialStr;
        }


        ExplainPrinter(const ExplainPrinter& other) = delete;

        ExplainPrinter& operator=(const ExplainPrinter& other) = delete;

        ExplainPrinter(ExplainPrinter&& other) noexcept
            : _cmd(std::move(other._cmd)),
              _os(std::move(other._os)),
              _osDirty(other._osDirty),
              _indentCount(other._indentCount),
              _childrenRemaining(other._childrenRemaining) {}

        template <class T>
        ExplainPrinter& operator<<(const T& t) {
            _os << t;
            _osDirty = true;
            return *this;
        }

        ExplainPrinter& operator<<(ExplainPrinter& other) {
            newLine();
            other.newLine();

            if (_childrenRemaining > 0) {
                _childrenRemaining--;
            } else {
                indent();
            }

            std::copy(other._cmd.cbegin(), other._cmd.cend(), std::back_inserter(_cmd));
            unIndent();

            return *this;
        }

        ExplainPrinter& operator<<(const MarkChildCount& mark) {
            if (version > 1) {
                _childrenRemaining = mark._childCount;
                indent("");
                for (int i = 0; i < _childrenRemaining - 1; i++) {
                    indent("|");
                }
            }
            return *this;
        }

        std::string str(const bool singleLine = false) {
            newLine();

            std::ostringstream os;
            std::vector<std::string> linePrefix;

            bool firstAddLine = true;
            for (const auto& cmd : _cmd) {
                switch (cmd._type) {
                    case CommandType::Indent:
                        linePrefix.push_back(cmd._str);
                        break;

                    case CommandType::Unindent: {
                        linePrefix.pop_back();
                        break;
                    }

                    case CommandType::AddLine: {
                        if (singleLine) {
                            if (!firstAddLine) {
                                os << " ";
                            }
                            os << cmd._str;
                        } else {
                            for (const std::string& element : linePrefix) {
                                if (!element.empty()) {
                                    os << element << ((version == 1) ? " " : "   ");
                                }
                            }
                            os << cmd._str << "\n";
                        }

                        firstAddLine = false;
                        break;
                    }

                    default: { MONGO_UNREACHABLE; }
                }
            }

            return os.str();
        }

    private:
        void indent(std::string s = " ") {
            newLine();
            _indentCount++;
            _cmd.emplace_back(CommandType::Indent, std::move(s));
        }

        void unIndent() {
            newLine();
            _indentCount--;
            _cmd.emplace_back(CommandType::Unindent, "");
        }

        void newLine() {
            if (!_osDirty) {
                return;
            }
            const std::string& str = _os.str();
            _cmd.emplace_back(CommandType::AddLine, str);
            _os.str("");
            _os.clear();
            _osDirty = false;
        }

        CommandVector _cmd;
        std::ostringstream _os;
        bool _osDirty;
        int _indentCount;
        int _childrenRemaining;
    };

public:
    ExplainGeneratorTransporter(const bool displayProperties, const cascades::Memo* memo)
        : _displayProperties(displayProperties), _memo(memo) {
        uassert(6624000,
                "Memo must be provided in order to display properties.",
                !_displayProperties || _memo != nullptr);
    }

    /**
     * Nodes
     */
    ExplainPrinter transport(const References& references, std::vector<ExplainPrinter> inResults) {
        ExplainPrinter printer;
        printer << "RefBlock: ";
        for (ExplainPrinter& inMp : inResults) {
            printer << inMp;
        }
        return printer;
    }

    ExplainPrinter transport(const ExpressionBinder& binders,
                             std::vector<ExplainPrinter> inResults) {
        ExplainPrinter printer;
        printer << "BindBlock:";

        std::map<std::string, ExplainPrinter> ordered;
        for (size_t idx = 0; idx < inResults.size(); ++idx) {
            ordered.emplace(binders.names()[idx], std::move(inResults[idx]));
        }

        for (auto& [name, child] : ordered) {
            ExplainPrinter local;
            local << "[" << name << "]" << child;
            printer << local;
        }

        return printer;
    }

    static void printFieldProjectionMap(ExplainPrinter& printer, const FieldProjectionMap& map) {
        std::map<FieldNameType, ProjectionName> ordered;
        if (!map._ridProjection.empty()) {
            ordered["<rid>"] = map._ridProjection;
        }
        if (!map._rootProjection.empty()) {
            ordered["<root>"] = map._rootProjection;
        }
        for (const auto& entry : map._fieldProjections) {
            ordered.insert(entry);
        }

        bool first = true;
        for (const auto& [fieldName, projectionName] : ordered) {
            if (first) {
                first = false;
            } else {
                printer << ", ";
            }
            printer << "'" << fieldName << "': '" << projectionName << "'";
        }
    }

    ExplainPrinter transport(const ScanNode& node, ExplainPrinter bindResult) {
        ExplainPrinter printer;
        printer << "Scan ['" << node.getScanDefName() << "']" << bindResult;
        return printer;
    }

    ExplainPrinter transport(const PhysicalScanNode& node, ExplainPrinter bindResult) {
        ExplainPrinter printer;

        printer << "PhysicalScan [{";
        printFieldProjectionMap(printer, node.getFieldProjectionMap());
        printer << "}, '" << node.getScanDefName() << "'";
        if (node.useParallelScan()) {
            printer << ", parallel";
        }
        printer << "]" << bindResult;

        return printer;
    }

    ExplainPrinter transport(const CoScanNode& node) {
        ExplainPrinter printer("CoScan []");
        return printer;
    }

    void printInterval(ExplainPrinter& printer, const IntervalRequirement& interval) {
        const BoundRequirement& lowBound = interval.getLowBound();
        const BoundRequirement& highBound = interval.getHighBound();

        printer << (lowBound.isInclusive() ? "[" : "(");
        printer << "'";
        if (lowBound.isInfinite()) {
            printer << "-Inf";
        } else {
            printer << generate(lowBound.getBound()).str(true /*singleLine*/);
        }

        printer << "', '";
        if (highBound.isInfinite()) {
            printer << "+Inf";
        } else {
            printer << generate(highBound.getBound()).str(true /*forIndexBounds*/);
        }

        printer << "'" << (highBound.isInclusive() ? "]" : ")");
    }

    void printInterval(ExplainPrinter& printer, const MultiKeyIntervalRequirement& interval) {
        bool first = true;
        for (const auto& entry : interval) {
            if (first) {
                first = false;
            } else {
                printer << ", ";
            }
            printInterval(printer, entry);
        }
    }

    template <class T>
    void printIntervalDNF(ExplainPrinter& printer, const T& intervals) {
        bool firstDisjunct = true;
        for (const auto& conjunction : intervals) {
            if (firstDisjunct) {
                firstDisjunct = false;
            } else {
                printer << " U ";
            }
            printer << "{";

            bool firstConjunct = true;
            for (const auto& interval : conjunction) {
                if (firstConjunct) {
                    firstConjunct = false;
                } else {
                    printer << " ^ ";
                }
                printer << "{";
                printInterval(printer, interval);
                printer << "}";
            }

            printer << "}";
        }
    }

    ExplainPrinter transport(const IndexScanNode& node, ExplainPrinter bindResult) {
        ExplainPrinter printer;

        printer << "IndexScan [{";
        printFieldProjectionMap(printer, node.getFieldProjectionMap());
        printer << "}, ";

        const auto& spec = node.getIndexSpecification();
        printer << "scanDefName: '" << spec.getScanDefName() << "', ";
        printer << "indexDefName: '" << spec.getIndexDefName() << "', ";

        printer << "intervals: {";
        printIntervalDNF(printer, spec.getIntervals());
        printer << "}";

        if (spec.isReverseOrder()) {
            printer << ", reversed.";
        }

        printer << "]" << bindResult;

        return printer;
    }

    ExplainPrinter transport(const SeekNode& node,
                             ExplainPrinter bindResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer;

        printer << "Seek [";
        printer << "ridProjection: '" << node.getRIDProjectionName() << "', {";
        printFieldProjectionMap(printer, node.getFieldProjectionMap());
        printer << "}, '" << node.getScanDefName() << "']" << MarkChildCount(2) << bindResult
                << refsResult;

        return printer;
    }

    ExplainPrinter transport(const MemoLogicalDelegatorNode& node) {
        ExplainPrinter printer;

        printer << "MemoLogicalDelegator [";
        printer << "groupId: " << node.getGroupId();
        printer << "]";

        return printer;
    }

    ExplainPrinter transport(const MemoPhysicalDelegatorNode& node) {
        ExplainPrinter printer;
        const auto id = node.getNodeId();

        if (_displayProperties) {
            const auto& group = _memo->getGroup(id._groupId);
            const auto& result = group._physicalNodes.at(id._index);
            const ABT& n = result->_node;

            ExplainPrinter nodePrinter = generate(n);
            if (n.template is<MemoPhysicalDelegatorNode>()) {
                // Handle delegation.
                return nodePrinter;
            }

            ExplainPrinter logPropPrinter = printProperties("Logical", group._logicalProperties);
            ExplainPrinter physPropPrinter = printProperties("Physical", result->_physProps);

            printer << "Properties [cost: " << result->_cost.toString() << "]" << MarkChildCount(3);
            printer << logPropPrinter << physPropPrinter << nodePrinter;
        } else {
            printer << "MemoPhysicalDelegator [";
            printer << "groupId: " << id._groupId << ", index: " << id._index;
            printer << "]";
        }

        return printer;
    }

    ExplainPrinter transport(const FilterNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter filterResult) {
        ExplainPrinter printer;
        printer << "Filter []" << MarkChildCount(2) << filterResult << childResult;
        return printer;
    }

    ExplainPrinter transport(const EvaluationNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter projectionResult) {
        ExplainPrinter printer;
        printer << "Evaluation []" << MarkChildCount(2) << projectionResult << childResult;
        return printer;
    }

    ExplainPrinter printPartialSchemaReqMap(const PartialSchemaRequirements& reqMap) {
        ExplainPrinter printer("Requirements map:");

        for (const auto& [key, req] : reqMap) {
            ExplainPrinter local;

            local << "ref projection: '" << key._projectionName << "', ";
            local << "path: '" << generate(key._path).str(true /*forIndexBounds*/) << "', ";

            if (req.hasBoundProjectionName()) {
                local << "bound projection: '" << req.getBoundProjectionName() << "', ";
            }

            local << "intervals: {";
            printIntervalDNF(local, req.getIntervals());
            local << "}";

            printer << local;
        }

        return printer;
    }

    ExplainPrinter transport(const SargableNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter bindResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer("Sargable []");
        printer << MarkChildCount(5);

        ExplainPrinter reqMapPrinter = printPartialSchemaReqMap(node.getReqMap());
        printer << reqMapPrinter;

        ExplainPrinter candidateIndexPrinter("Candidate indexes");

        std::set<std::string> orderedIndexDefName;
        for (const auto& entry : node.getCandidateIndexMap()) {
            orderedIndexDefName.insert(entry.first);
        }

        size_t candidateIndex = 0;
        for (const auto& indexDefName : orderedIndexDefName) {
            candidateIndex++;
            ExplainPrinter local;
            local << "candidate #" << candidateIndex << ": " << indexDefName << ", ";

            const auto& candidateIndexEntry = node.getCandidateIndexMap().at(indexDefName);
            local << "{";
            printFieldProjectionMap(local, candidateIndexEntry._fieldProjectionMap);
            local << "}, {";

            {
                std::set<int> orderedFields;
                for (const int fieldId : candidateIndexEntry._fieldsToCollate) {
                    orderedFields.insert(fieldId);
                }
                bool first = true;
                for (const int fieldId : orderedFields) {
                    if (first) {
                        first = false;
                    } else {
                        local << ", ";
                    }
                    local << fieldId;
                }
            }

            local << "}, {";
            printIntervalDNF(local, candidateIndexEntry._intervals);
            local << "}";

            candidateIndexPrinter << local;
        }

        printer << candidateIndexPrinter << bindResult << refsResult << childResult;
        return printer;
    }

    ExplainPrinter transport(const RIDIntersectNode& node,
                             ExplainPrinter leftChildResult,
                             ExplainPrinter rightChildResult) {
        ExplainPrinter printer("RIDIntersect []");
        // Print right child (inner) first, then left child (outer).
        printer << MarkChildCount(2) << rightChildResult << leftChildResult;
        return printer;
    }

    ExplainPrinter transport(const BinaryJoinNode& node,
                             ExplainPrinter leftChildResult,
                             ExplainPrinter rightChildResult,
                             ExplainPrinter filterResult) {
        ExplainPrinter printer;

        printer << "BinaryJoin [";
        printer << "joinType: " << JoinTypeEnum::toString[static_cast<int>(node.getJoinType())];
        if (!node.getCorrelatedProjectionNames().empty()) {
            printer << ", {";
            bool first = true;
            for (const ProjectionName& projectionName : node.getCorrelatedProjectionNames()) {
                if (first) {
                    first = false;
                } else {
                    printer << ", ";
                }
                printer << projectionName;
            }
            printer << "}";
        }
        printer << "]";

        // Print right child (inner) first, then left child (outer).
        printer << MarkChildCount(3) << filterResult << rightChildResult << leftChildResult;
        return printer;
    }

    ExplainPrinter transport(const HashJoinNode& node,
                             ExplainPrinter leftChildResult,
                             ExplainPrinter rightChildResult,
                             ExplainPrinter /*refsResult*/) {
        ExplainPrinter printer;

        printer << "HashJoin [";
        printer << "joinType: " << JoinTypeEnum::toString[static_cast<int>(node.getJoinType())];
        printer << "]";

        ExplainPrinter joinConditionPrinter("Condition");
        for (size_t i = 0; i < node.getLeftKeys().size(); i++) {
            ExplainPrinter local;
            local << node.getLeftKeys().at(i) << " = " << node.getRightKeys().at(i);
            joinConditionPrinter << local;
        }

        // Print right child (inner) first, then left child (outer).
        printer << MarkChildCount(3) << joinConditionPrinter << rightChildResult << leftChildResult;
        return printer;
    }

    ExplainPrinter transport(const InnerMultiJoinNode& node,
                             std::vector<ExplainPrinter> childResults) {
        ExplainPrinter printer;

        printer << "InnerMultiJoin []" << MarkChildCount(childResults.size());
        for (ExplainPrinter& childMp : childResults) {
            printer << childMp;
        }

        return printer;
    }

    ExplainPrinter transport(const UnionNode& node,
                             std::vector<ExplainPrinter> childResults,
                             ExplainPrinter bindResult,
                             ExplainPrinter /*refsResult*/) {
        ExplainPrinter printer;

        printer << "Union []" << MarkChildCount(childResults.size() + 1) << bindResult;
        for (ExplainPrinter& childMp : childResults) {
            printer << childMp;
        }

        return printer;
    }

    ExplainPrinter transport(const GroupByNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter bindAggResult,
                             ExplainPrinter refsAggResult,
                             ExplainPrinter bindGbResult,
                             ExplainPrinter refsGbResult) {
        ExplainPrinter printer;

        printer << "GroupBy [";
        if (node.isLocal()) {
            printer << "local";
        } else if (!node.canRewriteIntoLocal()) {
            printer << "global";
        }
        printer << "]";

        printer << MarkChildCount(3);
        {
            ExplainPrinter gbPrinter("groupings:");
            gbPrinter << refsGbResult;
            printer << gbPrinter;
        }

        {
            std::map<ProjectionName, size_t> ordered;
            const ProjectionNameVector& aggProjectionNames = node.getAggregationProjectionNames();
            for (size_t i = 0; i < aggProjectionNames.size(); i++) {
                ordered.emplace(aggProjectionNames.at(i), i);
            }

            ExplainPrinter aggPrinter("aggregations:");
            for (const auto& [projectionName, index] : ordered) {
                ExplainPrinter aggName;
                aggName << "[" << projectionName << "]";
                ExplainPrinter aggExpr = generate(node.getAggregationExpressions().at(index));
                aggName << aggExpr;
                aggPrinter << aggName;
            }

            printer << aggPrinter;
        }

        printer << childResult;
        return printer;
    }

    ExplainPrinter transport(const UnwindNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter bindResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer;

        printer << "Unwind [";
        if (node.getRetainNonArrays()) {
            printer << "retainNonArrays";
        }
        printer << "]";

        printer << MarkChildCount(2) << bindResult << childResult;
        return printer;
    }

    ExplainPrinter transport(const WindNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter bindResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer;

        printer << "Wind []" << MarkChildCount(2) << refsResult << childResult;
        return printer;
    }

    static void printCollationProperty(ExplainPrinter& parent,
                                       const properties::CollationRequirement& property) {
        ExplainPrinter propPrinter("collation:");
        for (const auto& entry : property.getCollationSpec()) {
            ExplainPrinter local;
            local << entry.first << ": "
                  << CollationOpEnum::toString[static_cast<int>(entry.second)];
            propPrinter << local;
        }
        parent << propPrinter;
    }

    ExplainPrinter transport(const CollationNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer("CollationNode []");

        printer << MarkChildCount(3);
        printCollationProperty(printer, node.getProperty());
        printer << refsResult << childResult;

        return printer;
    }

    static void printLimitSkipProperty(ExplainPrinter& parent,
                                       const properties::LimitSkipRequirement& property) {
        ExplainPrinter limitPrinter("limit: ");
        if (property.hasNoLimit()) {
            limitPrinter << "(none)";
        } else {
            limitPrinter << property.getLimit();
        }

        ExplainPrinter skipPrinter("skip: ");
        skipPrinter << property.getSkip();

        ExplainPrinter propPrinter;
        propPrinter << "limitSkip: [";
        if (property.isEnforced()) {
            propPrinter << "enforced";
        }
        propPrinter << "]";

        propPrinter << limitPrinter << skipPrinter;
        parent << propPrinter;
    }

    ExplainPrinter transport(const LimitSkipNode& node, ExplainPrinter childResult) {
        ExplainPrinter printer("LimitSkipNode []");

        printer << MarkChildCount(2);
        printLimitSkipProperty(printer, node.getProperty());
        printer << childResult;

        return printer;
    }

    static void printPropertyProjections(ExplainPrinter& parent,
                                         const ProjectionNameVector& projections) {
        ExplainPrinter printer("projections:");
        for (const ProjectionName& projection : projections) {
            ExplainPrinter local(projection);
            printer << local;
        }
        parent << printer;
    }

    static void printDistributionProperty(ExplainPrinter& parent,
                                          const properties::DistributionRequirement& property) {
        ExplainPrinter typePrinter("type: ");
        typePrinter << DistributionTypeEnum::toString[static_cast<int>(property.getType())];

        ExplainPrinter propPrinter("distribution:");
        propPrinter << typePrinter;
        printPropertyProjections(propPrinter, property.getProjections());
        parent << propPrinter;
    }

    static void printProjectionRequirementProperty(
        ExplainPrinter& parent, const properties::ProjectionRequirement& property) {
        printPropertyProjections(parent, property.getProjections().getVector());
    }

    ExplainPrinter transport(const ExchangeNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer;
        printer << "Exchange [";
        if (node.getPreserveSort()) {
            printer << "preserveSort";
        }
        printer << "]";

        printer << MarkChildCount(3);
        printDistributionProperty(printer, node.getProperty());
        printer << refsResult << childResult;

        return printer;
    }

    struct PropPrintVisitor {
        PropPrintVisitor(ExplainPrinter& parent) : _parent(parent){};

        void operator()(const properties::Property&, const properties::CollationRequirement& prop) {
            printCollationProperty(_parent, prop);
        }

        void operator()(const properties::Property&, const properties::LimitSkipRequirement& prop) {
            printLimitSkipProperty(_parent, prop);
        }

        void operator()(const properties::Property&,
                        const properties::ProjectionRequirement& prop) {
            printProjectionRequirementProperty(_parent, prop);
        }

        void operator()(const properties::Property&,
                        const properties::DistributionRequirement& prop) {
            printDistributionProperty(_parent, prop);
        }

        void operator()(const properties::Property&, const properties::IndexingRequirement& prop) {
            ExplainPrinter printer;

            printer << "Indexing requirement [";
            printer << properties::IndexReqTargetEnum::toString[static_cast<int>(
                prop.getIndexReqTarget())];
            if (!prop.getRIDProjectionName().empty()) {
                printer << ", ridProjection: " << prop.getRIDProjectionName();
            }
            printer << "]: ";

            _parent << printer;
        }

        void operator()(const properties::Property&, const properties::RepetitionEstimate& prop) {
            ExplainPrinter printer("Repetition estimate: ");
            printer << prop.getEstimate();
            _parent << printer;
        }

        void operator()(const properties::Property&,
                        const properties::ProjectionAvailability& prop) {
            ExplainPrinter printer("Logical projection set: ");
            ProjectionNameOrderedSet ordered;
            for (const ProjectionName& projection : prop.getProjections()) {
                ordered.insert(projection);
            }
            for (const ProjectionName& projection : ordered) {
                ExplainPrinter local(projection);
                printer << local;
            }
            _parent << printer;
        }

        void operator()(const properties::Property&, const properties::CardinalityEstimate& prop) {
            ExplainPrinter printer("Logical cardinality estimate: ");
            printer << prop.getEstimate();
            _parent << printer;
        }

        void operator()(const properties::Property&, const properties::IndexingAvailability& prop) {
            ExplainPrinter printer("Indexing potential: ");
            printer << "[groupId: " << prop.getScanGroupId()
                    << ", scanProjection: " << prop.getScanProjection() << "]";

            if (!prop.getSargableProjectionNames().empty()) {
                ExplainPrinter sargableProjNamesPrinter("Sargable projection names: ");
                for (size_t i = 0; i < prop.getSargableProjectionNames().size(); i++) {
                    const auto& projSet = prop.getSargableProjectionNames().at(i);
                    ExplainPrinter projSetPrinter;
                    projSetPrinter << "Position #" << i;

                    for (const ProjectionName& projectionName : projSet.getVector()) {
                        ExplainPrinter local(projectionName);
                        projSetPrinter << local;
                    }

                    sargableProjNamesPrinter << projSetPrinter;
                }
                printer << sargableProjNamesPrinter;
            }

            _parent << printer;
        }

        void operator()(const properties::Property&,
                        const properties::CollectionAvailability& prop) {
            ExplainPrinter printer("Collection availability: ");

            std::set<std::string> orderedSet;
            for (const std::string& scanDef : prop.getScanDefSet()) {
                orderedSet.insert(scanDef);
            }
            for (const std::string& scanDef : orderedSet) {
                ExplainPrinter local(scanDef);
                printer << local;
            }

            _parent << printer;
        }

        void operator()(const properties::Property&,
                        const properties::DistributionAvailability& prop) {
            ExplainPrinter printer("Distribution availability: ");

            struct Comparator {
                bool operator()(const properties::DistributionRequirement& d1,
                                const properties::DistributionRequirement& d2) const {
                    if (d1.getType() < d2.getType()) {
                        return true;
                    }
                    if (d1.getType() > d2.getType()) {
                        return false;
                    }
                    return d1.getProjections() < d2.getProjections();
                }
            };

            std::set<properties::DistributionRequirement, Comparator> ordered;
            for (const auto& distributionProp : prop.getDistributionSet()) {
                ordered.insert(distributionProp);
            }

            for (const auto& distributionProp : ordered) {
                ExplainPrinter local;
                printDistributionProperty(local, distributionProp);
                printer << local;
            }

            _parent << printer;
        }

    private:
        // We don't own this.
        ExplainPrinter& _parent;
    };

    static ExplainPrinter printProperties(const std::string& description,
                                          const properties::Properties& props) {
        ExplainPrinter propertiesPrinter;
        propertiesPrinter << description << ":";

        std::map<properties::Property::key_type, properties::Property> ordered;
        for (const auto& entry : props) {
            ordered.template insert(entry);
        }

        PropPrintVisitor visitor(propertiesPrinter);
        for (const auto& entry : ordered) {
            entry.second.visit(visitor);
        }

        return propertiesPrinter;
    }

    ExplainPrinter transport(const RootNode& node,
                             ExplainPrinter childResult,
                             ExplainPrinter refsResult) {
        ExplainPrinter printer;

        printer << "RootNode []" << MarkChildCount(3);
        printProjectionRequirementProperty(printer, node.getProperty());
        printer << refsResult << childResult;
        return printer;
    }

    /**
     * Expressions
     */
    ExplainPrinter transport(const Blackhole& expr) {
        return ExplainPrinter("Blackhole []");
    }

    ExplainPrinter transport(const Constant& expr) {
        ExplainPrinter printer;
        printer << "Const [" << expr.get() << "]";
        return printer;
    }

    ExplainPrinter transport(const Variable& expr) {
        ExplainPrinter printer;
        printer << "Variable [" << expr.name() << "]";
        return printer;
    }

    ExplainPrinter transport(const UnaryOp& expr, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "UnaryOp [" << OperationsEnum::toString[static_cast<int>(expr.op())] << "]"
                << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const BinaryOp& expr,
                             ExplainPrinter leftResult,
                             ExplainPrinter rightResult) {
        ExplainPrinter printer;
        printer << "BinaryOp [" << OperationsEnum::toString[static_cast<int>(expr.op())] << "]"
                << MarkChildCount(2) << leftResult << rightResult;
        return printer;
    }


    ExplainPrinter transport(const If& expr,
                             ExplainPrinter condResult,
                             ExplainPrinter thenResult,
                             ExplainPrinter elseResult) {
        ExplainPrinter printer;
        printer << "If []" << MarkChildCount(3) << condResult << thenResult << elseResult;
        return printer;
    }

    ExplainPrinter transport(const Let& expr,
                             ExplainPrinter bindResult,
                             ExplainPrinter exprResult) {
        ExplainPrinter printer;
        printer << "Let [" << expr.varName() << "]" << MarkChildCount(2) << bindResult
                << exprResult;
        return printer;
    }

    ExplainPrinter transport(const LambdaAbstraction& expr, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "LambdaAbstraction [" << expr.varName() << "]" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const LambdaApplication& expr,
                             ExplainPrinter lambdaResult,
                             ExplainPrinter argumentResult) {
        ExplainPrinter printer;
        printer << "LambdaApplication []" << MarkChildCount(2) << lambdaResult << argumentResult;
        return printer;
    }

    ExplainPrinter transport(const FunctionCall& expr, std::vector<ExplainPrinter> argResults) {
        ExplainPrinter printer;
        printer << "FunctionCall [" << expr.name() << "]" << MarkChildCount(argResults.size());
        for (ExplainPrinter& argPrinter : argResults) {
            printer << argPrinter;
        }
        return printer;
    }

    ExplainPrinter transport(const EvalPath& expr,
                             ExplainPrinter pathResult,
                             ExplainPrinter inputResult) {
        ExplainPrinter printer;
        printer << "EvalPath []" << MarkChildCount(2) << pathResult << inputResult;
        return printer;
    }

    ExplainPrinter transport(const EvalFilter& expr,
                             ExplainPrinter pathResult,
                             ExplainPrinter inputResult) {
        ExplainPrinter printer;
        printer << "EvalFilter []" << MarkChildCount(2) << pathResult << inputResult;
        return printer;
    }

    /**
     * Paths
     */
    ExplainPrinter transport(const PathConstant& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathConstant []" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathLambda& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathLambda []" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathIdentity& path) {
        return ExplainPrinter("PathIdentity []");
    }

    ExplainPrinter transport(const PathDefault& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathDefault [] " << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathCompare& path, ExplainPrinter valueResult) {
        ExplainPrinter printer;
        printer << "PathCompare [" << OperationsEnum::toString[static_cast<int>(path.op())] << "] "
                << MarkChildCount(1) << valueResult;
        return printer;
    }

    static void printPathProjections(ExplainPrinter& printer,
                                     const std::unordered_set<std::string>& names) {
        bool first = true;
        std::set<std::string> ordered;
        for (const std::string& s : names) {
            ordered.insert(s);
        }

        for (const std::string& s : ordered) {
            if (first) {
                first = false;
            } else {
                printer << ", ";
            }
            printer << s;
        }

        printer << "]";
    }

    ExplainPrinter transport(const PathDrop& path) {
        ExplainPrinter printer("PathDrop [");
        printPathProjections(printer, path.getNames());
        return printer;
    }

    ExplainPrinter transport(const PathKeep& path) {
        ExplainPrinter printer("PathKeep [");
        printPathProjections(printer, path.getNames());
        return printer;
    }

    ExplainPrinter transport(const PathObj& path) {
        return ExplainPrinter("PathObj [] ");
    }

    ExplainPrinter transport(const PathArr& path) {
        return ExplainPrinter("PathArr [] ");
    }

    ExplainPrinter transport(const PathTraverse& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathTraverse []" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathField& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathField [" << path.name() << "]" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathGet& path, ExplainPrinter inResult) {
        ExplainPrinter printer;
        printer << "PathGet [" << path.name() << "]" << MarkChildCount(1) << inResult;
        return printer;
    }

    ExplainPrinter transport(const PathComposeM& path,
                             ExplainPrinter leftResult,
                             ExplainPrinter rightResult) {
        ExplainPrinter printer;
        printer << "PathComposeM []" << MarkChildCount(2) << leftResult << rightResult;
        return printer;
    }

    ExplainPrinter transport(const PathComposeA& path,
                             ExplainPrinter leftResult,
                             ExplainPrinter rightResult) {
        ExplainPrinter printer;
        printer << "PathComposeA []" << MarkChildCount(2) << leftResult << rightResult;
        return printer;
    }

    ExplainPrinter transport(const Source& expr) {
        return ExplainPrinter("Source []");
    }

    ExplainPrinter generate(const ABT& node) {
        return algebra::transport<false>(node, *this);
    }

    ExplainPrinter printMemo() {
        ExplainPrinter printer("Memo");

        for (size_t groupId = 0; groupId < _memo->getGroupCount(); groupId++) {
            const cascades::Group& group = _memo->getGroup(groupId);

            ExplainPrinter groupPrinter;
            groupPrinter << "Group #" << groupId;
            ExplainPrinter logicalPropPrinter =
                printProperties("Logical properties", group._logicalProperties);
            groupPrinter << logicalPropPrinter;

            {
                ExplainPrinter logicalNodePrinter("Logical nodes");
                const ABTVector& logicalNodes = group._logicalNodes.getVector();
                for (size_t i = 0; i < logicalNodes.size(); i++) {
                    ExplainPrinter local;
                    local << "LogicalNode #" << i;
                    ExplainPrinter nodePrinter = generate(logicalNodes.at(i));
                    local << nodePrinter;
                    logicalNodePrinter << local;
                }
                groupPrinter << logicalNodePrinter;
            }

            {
                ExplainPrinter physicalNodePrinter("Physical nodes");
                for (const auto& physOptResult : group._physicalNodes) {
                    ExplainPrinter local;
                    local << "PhysicalNode #" << physOptResult->_index
                          << ", cost limit: " << physOptResult->_costLimit.toString();

                    if (physOptResult->hasResult()) {
                        local << ", cost: " << physOptResult->_cost.toString();
                        ExplainPrinter nodePrinter = generate(physOptResult->_node);
                        local << nodePrinter;
                    } else {
                        local << " (failed to optimize)";
                    }

                    ExplainPrinter propPrinter =
                        printProperties("Physical properties", physOptResult->_physProps);
                    local << propPrinter;

                    physicalNodePrinter << local;
                }
                groupPrinter << physicalNodePrinter;
            }

            printer << groupPrinter;
        }

        return printer;
    }

private:
    const bool _displayProperties;

    // We don't own this.
    const cascades::Memo* _memo;
};

std::string ExplainGenerator::explain(const ABT& node,
                                      const bool displayProperties,
                                      const cascades::Memo* memo) {
    ExplainGeneratorTransporter gen(displayProperties, memo);
    return gen.generate(node).str();
}

std::string ExplainGenerator::explainV2(const ABT& node,
                                        bool displayProperties,
                                        const cascades::Memo* memo) {
    ExplainGeneratorTransporter<2> gen(displayProperties, memo);
    return gen.generate(node).str();
}

std::string ExplainGenerator::explainProperties(const std::string& description,
                                                const properties::Properties& properties) {
    return ExplainGeneratorTransporter<2>::printProperties(description, properties).str();
}

std::string ExplainGenerator::explainMemo(const cascades::Memo& memo) {
    ExplainGeneratorTransporter<2> gen(false /*displayProperties*/, &memo);
    return gen.printMemo().str();
}

std::string ExplainGenerator::explainPartialSchemaReqMap(const PartialSchemaRequirements& reqMap) {
    ExplainGeneratorTransporter<2> gen(false /*displayProperties*/, nullptr);
    return gen.printPartialSchemaReqMap(reqMap).str();
}

}  // namespace mongo::optimizer
