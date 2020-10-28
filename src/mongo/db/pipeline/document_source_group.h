/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#include <memory>
#include <utility>

#include "mongo/db/index/sort_key_generator.h"
#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/transformer_interface.h"
#include "mongo/db/sorter/sorter.h"

namespace mongo {

/**
 * GroupFromFirstTransformation consists of a list of (field name, expression pairs). It returns a
 * document synthesized by assigning each field name in the output document to the result of
 * evaluating the corresponding expression. If the expression evaluates to missing, we assign a
 * value of BSONNULL. This is necessary to match the semantics of $first for missing fields.
 */
class GroupFromFirstDocumentTransformation final : public TransformerInterface {
public:
    GroupFromFirstDocumentTransformation(
        const std::string& groupId,
        std::vector<std::pair<std::string, boost::intrusive_ptr<Expression>>> accumulatorExprs)
        : _accumulatorExprs(std::move(accumulatorExprs)), _groupId(groupId) {}

    TransformerType getType() const final {
        return TransformerType::kGroupFromFirstDocument;
    }

    /**
     * The path of the field that we are grouping on: i.e., the field in the input document that we
     * will use to create the _id field of the ouptut document.
     */
    const std::string& groupId() const {
        return _groupId;
    }

    Document applyTransformation(const Document& input) final;

    void optimize() final;

    Document serializeTransformation(
        boost::optional<ExplainOptions::Verbosity> explain) const final;

    DepsTracker::State addDependencies(DepsTracker* deps) const final;

    DocumentSource::GetModPathsReturn getModifiedPaths() const final;

    static std::unique_ptr<GroupFromFirstDocumentTransformation> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const std::string& groupId,
        std::vector<std::pair<std::string, boost::intrusive_ptr<Expression>>> accumulatorExprs);

private:
    std::vector<std::pair<std::string, boost::intrusive_ptr<Expression>>> _accumulatorExprs;
    std::string _groupId;
};

class DocumentSourceGroup : public DocumentSource {
public:
    using Accumulators = std::vector<boost::intrusive_ptr<AccumulatorState>>;
    using GroupsMap = ValueUnorderedMap<Accumulators>;

    static constexpr StringData kStageName = "$group"_sd;

    DocumentSourceGroup(const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSourceGroup(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                        const boost::intrusive_ptr<Expression>& groupByExpression,
                        std::vector<AccumulationStatement> accumulationStatements,
                        boost::optional<size_t> maxMemoryUsageBytes = boost::none);

    virtual ~DocumentSourceGroup();

    boost::intrusive_ptr<DocumentSource> optimize() final;
    DepsTracker::State getDependencies(DepsTracker* deps) const final;
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;
    const char* getSourceName() const final;
    GetModPathsReturn getModifiedPaths() const final;
    StringMap<boost::intrusive_ptr<Expression>> getIdFields() const;
    const std::vector<AccumulationStatement>& getAccumulatedFields() const;

    /**
     * Convenience method for creating a new $group stage. If maxMemoryUsageBytes is boost::none,
     * then it will actually use the value of internalDocumentSourceGroupMaxMemoryBytes.
     */
    static boost::intrusive_ptr<DocumentSourceGroup> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const boost::intrusive_ptr<Expression>& groupByExpression,
        std::vector<AccumulationStatement> accumulationStatements,
        boost::optional<size_t> maxMemoryUsageBytes = boost::none) {
        return make_intrusive<DocumentSourceGroup>(
            expCtx, groupByExpression, std::move(accumulationStatements), maxMemoryUsageBytes);
    }

    /**
     * Parses 'elem' into a $group stage, or throws a AssertionException if 'elem' was an invalid
     * specification.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kBlocking,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kWritesTmpData,
                                     FacetRequirement::kAllowed,
                                     TransactionRequirement::kAllowed,
                                     LookupRequirement::kAllowed,
                                     UnionRequirement::kAllowed);
        constraints.canSwapWithMatch = true;
        return constraints;
    }

    /**
     * Add an accumulator, which will become a field in each Document that results from grouping.
     */
    void addAccumulator(AccumulationStatement accumulationStatement);

    /**
     * Sets the expression to use to determine the group id of each document.
     */
    void setIdExpression(const boost::intrusive_ptr<Expression> idExpression);

    /**
     * Returns true if this $group stage represents a 'global' $group which is merging together
     * results from earlier partial groups.
     */
    bool doingMerge() const {
        return _doingMerge;
    }

    /**
     * Tell this source if it is doing a merge from shards. Defaults to false.
     */
    void setDoingMerge(bool doingMerge) {
        _doingMerge = doingMerge;
    }

    /**
     * Returns true if this $group stage used disk during execution and false otherwise.
     */
    bool usedDisk() final;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final;
    bool canRunInParallelBeforeWriteStage(
        const std::set<std::string>& nameOfShardKeyFieldsUponEntryToStage) const final;

    /**
     * When possible, creates a document transformer that transforms the first document in a group
     * into one of the output documents of the $group stage. This is possible when we are grouping
     * on a single field and all accumulators are $first (or there are no accumluators).
     *
     * It is sometimes possible to use a DISTINCT_SCAN to scan the first document of each group,
     * in which case this transformation can replace the actual $group stage in the pipeline
     * (SERVER-9507).
     */
    std::unique_ptr<GroupFromFirstDocumentTransformation> rewriteGroupAsTransformOnFirstDocument()
        const;

    Pipeline::SourceContainer::iterator doOptimizeAt(Pipeline::SourceContainer::iterator itr,
                                                     Pipeline::SourceContainer* container) final;

protected:
    GetNextResult doGetNext() override;
    void doDispose() final;

    /**
     * Unloads the next item from '_groups' or the spilled files. Returns EOF if everything has been
     * returned already.
     */
    GetNextResult unloadGroupedResult(bool disposeIfFinished = true);

    /**
     * Prepares to return results out of '_groups' or the spilled files by setting up iteration
     * state like '_groupsIterator' or '_sortIterator'.
     */
    void markEndOfLoading();

    /**
     * Inserts or merges 'rootDocument' into '_groups'. This method handles spilling to disk and
     * associated memory usage tracking.
     */
    void insertToGroupsMap(Document&& rootDocument);

    Document makeDocument(const Value& id, const Accumulators& accums, bool mergeableOutput);

    std::vector<AccumulationStatement> _accumulatedFields;

    // We use boost::optional to defer initialization until the ExpressionContext containing the
    // correct comparator is injected, since the groups must be built using the comparator's
    // definition of equality.
    boost::optional<GroupsMap> _groups;

    // Only used when '_spilled' is false.
    GroupsMap::iterator _groupsIterator;

private:
    struct MemoryUsageTracker {
        /**
         * Cleans up any pending memory usage. Throws error, if memory usage is above
         * 'maxMemoryUsageBytes' and cannot spill to disk. The 'saveMemory' function should return
         * the amount of memory saved by the cleanup.
         *
         * Returns true, if the caller should spill to disk, false otherwise.
         */
        bool shouldSpillWithAttemptToSaveMemory(std::function<int()> saveMemory);

        const bool allowDiskUse;
        const size_t maxMemoryUsageBytes;
        size_t memoryUsageBytes = 0;
    };

    /**
     * getNext() dispatches to one of these three depending on what type of $group it is. These
     * methods expect '_currentAccumulators' to have been reset before being called, and also expect
     * initialize() to have been called already.
     */
    GetNextResult getNextSpilled(bool disposeIfFinished);
    GetNextResult getNextStandard(bool disposeIfFinished);

    /**
     * Before returning anything, this source must prepare itself. In a streaming $group,
     * initialize() requests the first document from the previous source, and uses it to prepare the
     * accumulators. In an unsorted $group, initialize() exhausts the previous source before
     * returning. The '_initialized' boolean indicates that initialize() has finished.
     *
     * This method may not be able to finish initialization in a single call if 'pSource' returns a
     * DocumentSource::GetNextResult::kPauseExecution, so it returns the last GetNextResult
     * encountered, which may be either kEOF or kPauseExecution.
     */
    GetNextResult initialize();

    /**
     * Spill groups map to disk and returns an iterator to the file. Note: Since a sorted $group
     * does not exhaust the previous stage before returning, and thus does not maintain as large a
     * store of documents at any one time, only an unsorted group can spill to disk.
     */
    std::shared_ptr<Sorter<Value, Value>::Iterator> spill();

    /**
     * If we ran out of memory, finish all the pending operations so that some memory
     * can be freed.
     */
    int freeMemory();

    /**
     * Computes the internal representation of the group key.
     */
    Value computeId(const Document& root);

    /**
     * Converts the internal representation of the group key to the _id shape specified by the
     * user.
     */
    Value expandId(const Value& val);

    /**
     * Returns true if 'dottedPath' is one of the group keys present in '_idExpressions'.
     */
    bool pathIncludedInGroupKeys(const std::string& dottedPath) const;

    bool _usedDisk;  // Keeps track of whether this $group spilled to disk.
    bool _doingMerge;

    MemoryUsageTracker _memoryTracker;

    std::string _fileName;
    std::streampos _nextSortedFileWriterOffset = 0;
    bool _ownsFileDeletion = true;  // unless a MergeIterator is made that takes over.

    std::vector<std::string> _idFieldNames;  // used when id is a document
    std::vector<boost::intrusive_ptr<Expression>> _idExpressions;

    bool _initialized;

    Value _currentId;
    Accumulators _currentAccumulators;

    std::vector<std::shared_ptr<Sorter<Value, Value>::Iterator>> _sortedFiles;
    bool _spilled;

    // Only used when '_spilled' is true.
    std::unique_ptr<Sorter<Value, Value>::Iterator> _sorterIterator;

    std::pair<Value, Value> _firstPartOfNextGroup;

    DocumentSource::Sorts _inputSorts;
};

class FakeFieldNameGenerator {
public:
    StringData next() {
        using namespace fmt::literals;
        return fmt::format("f{}", _count++);
    }

private:
    uint64_t _count = 0;
};

class DocumentSourceSemiStreamingGroup : public DocumentSourceGroup {
public:
    struct GroupByPart {
        boost::intrusive_ptr<Expression> expression;
        boost::optional<SortDirection> sortDirection;

        GroupByPart() = default;
        GroupByPart(boost::intrusive_ptr<Expression> expression,
                    boost::optional<SortDirection> sortDirection)
            : expression(std::move(expression)), sortDirection(sortDirection) {}
    };

    static boost::intrusive_ptr<DocumentSourceSemiStreamingGroup> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        boost::intrusive_ptr<Expression> idExpression,
        std::vector<GroupByPart> groupKeys,
        std::vector<AccumulationStatement> accumulationStatements,
        boost::optional<size_t> maxMemoryUsageBytes = boost::none) {
        return make_intrusive<DocumentSourceSemiStreamingGroup>(expCtx,
                                                                std::move(idExpression),
                                                                std::move(groupKeys),
                                                                std::move(accumulationStatements),
                                                                maxMemoryUsageBytes);
    }

    DocumentSourceSemiStreamingGroup(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                     boost::intrusive_ptr<Expression> idExpression,
                                     std::vector<GroupByPart> groupKeys,
                                     std::vector<AccumulationStatement> accumulationStatements,
                                     boost::optional<size_t> maxMemoryUsageBytes)
        : DocumentSourceGroup(expCtx,
                              std::move(idExpression),
                              std::move(accumulationStatements),
                              maxMemoryUsageBytes),
          _groupKeys(std::move(groupKeys)),
          _sortPattern([this] {
              std::vector<SortPattern::SortPatternPart> sortPatternParts;
              FakeFieldNameGenerator fakeFieldNames;
              for (auto&& key : _groupKeys) {
                  if (auto direction = key.sortDirection) {
                      sortPatternParts.emplace_back(
                          *key.sortDirection, FieldPath{fakeFieldNames.next()}, nullptr);
                  }
              }
              invariant(!sortPatternParts.empty(),
                        "Should have at least one sorted input to use streaming group");
              return sortPatternParts;
          }()),
          _sortKeyGen(_sortPattern, expCtx->getCollator()) {}

    ~DocumentSourceSemiStreamingGroup() {}

protected:
    GetNextResult doGetNext() final {
        if (_firstDocForNextGroup) {
            // We're in the process of unloading the current map.
            auto nextFromMap = releaseValuesFromMap();
            if (nextFromMap.isAdvanced()) {
                return nextFromMap;
            }
            invariant(nextFromMap.isEOF(), "Never expect to generate pauses while unloading map");
        }

        auto next = pSource->getNext();
        boost::optional<Document> readyResult;
        while (next.isAdvanced()) {
            std::tie(next, readyResult) = processNewDocument(next.releaseDocument());
            if (readyResult) {
                invariant(next.isEOF());
                std::cout << "CHARLIE (ready) " << readyResult->toBson() << std::endl;
                return std::move(*readyResult);
            }
        }

        markEndOfLoading();
        switch (next.getStatus()) {
            case GetNextResult::ReturnStatus::kEOF: {
                return releaseValuesFromMap();
            }
            case GetNextResult::ReturnStatus::kPauseExecution: {
                return next;
            }
            default: { MONGO_UNREACHABLE; }
        }
    }

private:
    Value getSortKeyForSortedGroupKeys(const Document& doc) {
        MutableDocument mockForSorter;
        FakeFieldNameGenerator fakeFieldNames;
        for (auto&& key : _groupKeys) {
            if (key.sortDirection) {
                mockForSorter.addField(fakeFieldNames.next(),
                                       key.expression->evaluate(doc, &pExpCtx->variables));
            }
        }
        std::cout << "CHARLIE (sort) " << mockForSorter.peek().toBson() << std::endl;
        return _sortKeyGen.computeSortKeyFromDocument(mockForSorter.freeze());
    }

    std::pair<GetNextResult, boost::optional<Document>> processNewDocument(Document&& nextResult) {
        std::cout << "CHARLIE (input) " << nextResult.toBson() << std::endl;
        auto thisSortKey = getSortKeyForSortedGroupKeys(nextResult);
        std::cout << "CHARLIE (sortKey) " << thisSortKey.toString() << std::endl;
        if (!_currentSortKey) {
            _currentSortKey = std::move(thisSortKey);
            insertToGroupsMap(std::move(nextResult));
            return {pSource->getNext(), boost::none};
        }

        // The sort keys are already collation-aware, so make sure to compare here using the default
        // comparator.
        if (ValueComparator::kInstance.evaluate(thisSortKey == *_currentSortKey)) {
            insertToGroupsMap(std::move(nextResult));
            return {pSource->getNext(), boost::none};
        }

        // Next sort key spotted. We must have had at least one document in the group which just
        // ended, so return one of them.
        _currentSortKey = std::move(thisSortKey);
        _firstDocForNextGroup = std::move(nextResult);
        markEndOfLoading();
        auto unloadResult = unloadGroupedResult(false);
        invariant(unloadResult.isAdvanced());
        return {GetNextResult::makeEOF(), unloadResult.releaseDocument()};
    }

    GetNextResult releaseValuesFromMap() {
        auto nextResult = unloadGroupedResult(false);
        if (nextResult.isEOF() && _firstDocForNextGroup) {
            std::cout << "CHARLIE (next group starting) " << _firstDocForNextGroup->toBson()
                      << std::endl;
            insertToGroupsMap(std::move(*_firstDocForNextGroup));
            _firstDocForNextGroup = boost::none;
        }
        if (nextResult.isAdvanced()) {
            std::cout << "CHARLIE (unloaded) " << nextResult.getDocument().toBson() << std::endl;
        } else {
            std::cout << "CHARLIE (unloaded nothing) " << std::endl;
        }
        return nextResult;
    }

    // TODO: these data structures aren't huge, but maybe should count toward memory usage limit?
    std::vector<GroupByPart> _groupKeys;
    SortPattern _sortPattern;
    SortKeyGenerator _sortKeyGen;
    boost::optional<Value> _currentSortKey = boost::none;
    boost::optional<Document> _firstDocForNextGroup = boost::none;
};

}  // namespace mongo
